use anyhow::Result;
use log::{error, info};
use std::io::BufRead;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_graceful_shutdown::SubsystemBuilder;
use tokio_graceful_shutdown::Toplevel;

use std::fs::File;
use std::io::BufReader;
use tokio::task;

use std::sync::Arc;
use std::sync::RwLock;

use tokio_graceful_shutdown::SubsystemHandle;

use gridbuffer::core::gridbuffer::GridBuffer;

use droplet_core::grid_sample::GridRow;
use droplet_core::grid_sample::GridSample;

use droplet_core::db::db::DB;
use droplet_core::id_mapping::IDMapping;
use droplet_core::local_file_reader::{get_test_gridbuffer_filenames, LocalFileReader};
use droplet_core::window_heap::WindowHeap;

/// `GridSinker` is responsible for sorting `gridbuffer` data and sending it to the target worker node.
///
/// We use `WindowHeap` to sort `gridbuffer` data.
///
/// One `GridSinker` instance is responsible for one table.
///
/// Should we merge `gridbuffer` into larger `gridbuffer`?
///
/// Merging `gridbuffer` would reduce the total bytes when serialized, but the reduce of bytes is limited.
/// Besides, merging would be more expensive, and require more memory and computation.
///
/// So we don't merge `gridbuffer` in `GridSinker`.
pub struct GridSinker<T: Iterator<Item = Result<String>>> {
    /// table name. it should be global unique.
    table_name: String,

    /// `reader` to read `gridbuffer` data.
    reader: T,

    /// `window_heap` to sort `gridbuffer` data by `SampleKey`.
    window_heap: WindowHeap,

    /// ID mapping from string to u32.
    id_mapping: Arc<RwLock<IDMapping>>,
}

impl<T: Iterator<Item = Result<String>>> GridSinker<T> {
    /// Create a new GridSinker instance using a BufRead.
    pub fn new(table_name: &String, reader: T, id_mapping: Arc<RwLock<IDMapping>>) -> Self {
        let batch_size = 4;
        Self {
            table_name: table_name.clone(),
            reader,
            window_heap: WindowHeap::new(2, batch_size),
            id_mapping,
        }
    }

    /// Start the GridSinker process.
    pub async fn run(mut self, subsys: SubsystemHandle) -> Result<()> {
        info!("GridSinker process started");

        let gridbuffers = self.reader.filter_map(|line| match line {
            Ok(line) => match GridBuffer::from_base64(&line) {
                Ok(gridbuffer) => Some(gridbuffer),
                Err(e) => {
                    error!("Failed to parse gridbuffer, error: {}", e);
                    None
                }
            },
            Err(e) => {
                error!("Failed to read line, error: {}", e);
                None
            }
        });

        for gridbuffer in gridbuffers {
            info!(
                "Received gridbuffer, size: {}",
                gridbuffer.estimated_bytes()
            );

            self.window_heap.push(gridbuffer)?;

            if self.window_heap.out_gridbuffers().len() > 0 {
                while let Some(gridbuffer) = self.window_heap.get_out_gridbuffer() {
                    info!("Get out gridbuffer, size: {}", gridbuffer.estimated_bytes());
                }
            }
        }

        info!("GridSinker process done");
        Ok(())
    }

    /// Start local file sinker with `num_threads` threads.
    pub async fn start_local_file_sinker(table_name: &String, num_threads: usize) -> Result<()> {
        let filenames = get_test_gridbuffer_filenames(num_threads);

        let chunk_size = (filenames.len() + num_threads - 1) / num_threads;

        let mut handlers = Vec::new();

        let id_mapping = Arc::new(RwLock::new(IDMapping::new()?));

        for chunk in filenames.chunks(chunk_size) {
            let chunk_files = chunk.to_vec();

            let reader = LocalFileReader::new(&chunk_files)?;
            let sinker = GridSinker::new(table_name, reader, id_mapping.clone());

            let handler = task::spawn(async move {
                Toplevel::new(|s| async move {
                    s.start(SubsystemBuilder::new("sinker", |a| sinker.run(a)));
                })
                .catch_signals()
                .handle_shutdown_requests(Duration::from_millis(1000))
                .await
            });

            handlers.push(handler);
        }

        for handler in handlers {
            handler.await?;
        }

        Ok(())
    }
}
