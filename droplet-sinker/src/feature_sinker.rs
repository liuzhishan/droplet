use anyhow::Result;
use gridbuffer::core::performance::parse_simple_features;
use log::{error, info};
use std::io::BufRead;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_graceful_shutdown::SubsystemBuilder;
use tokio_graceful_shutdown::Toplevel;

use std::fs::File;
use std::io::BufReader;
use tokio::task;

use tokio_graceful_shutdown::SubsystemHandle;

use gridbuffer::core::feature_batcher::SimpleFeaturesBatcher;
use gridbuffer::core::gridbuffer::GridBuffer;

use droplet_core::local_file_reader::{get_test_feature_filenames, LocalFileReader};

/// `FeatureSinker` is responsible for converting SimpleFeatures into GridBuffer format
/// and sending it to the appropriate worker node.
///
/// One `FeatureSinker` instance is responsible for one table.
pub struct FeatureSinker<T: Iterator<Item = Result<String>>> {
    table_name: String,
    reader: T,
    num_rows: usize,
    num_cols: usize,
}

impl<T: Iterator<Item = Result<String>>> FeatureSinker<T> {
    /// Create a new FeatureSinker instance using a BufRead.
    pub fn new(table_name: &String, reader: T, num_rows: usize, num_cols: usize) -> Self {
        Self {
            table_name: table_name.clone(),
            reader,
            num_rows,
            num_cols,
        }
    }

    /// Start the FeatureSinker process
    pub async fn run(self, subsys: SubsystemHandle) -> Result<()> {
        info!("Starting FeatureSinker process");

        let features = self.reader.filter_map(|line| match line {
            Ok(line) => match parse_simple_features(&line) {
                Ok(features) => {
                    info!(
                        "Received features, llsid: {}, user_id: {}, item_id: {}, timestamp: {}",
                        features.llsid, features.user_id, features.item_id, features.timestamp
                    );
                    Some(features)
                }
                Err(e) => {
                    error!("Failed to parse simple features, error: {}", e);
                    None
                }
            },
            Err(e) => {
                error!("Failed to read line, error: {}", e);
                None
            }
        });

        let batcher = SimpleFeaturesBatcher::new(features, self.num_rows);

        for batch in batcher {
            info!("read one batch, bytes: {}", batch.estimated_bytes());
        }

        info!("Sinker process completed");
        Ok(())
    }

    /// Process and send data to the appropriate worker node.
    fn process_and_send_data(self, data: String) -> Result<()> {
        // TODO: Implement the main logic for processing and sending data
        Ok(())
    }

    /// Start local file sinker with `num_threads` threads.
    pub async fn start_local_file_sinker(
        table_name: &String,
        num_rows: usize,
        num_cols: usize,
        num_threads: usize,
    ) -> Result<()> {
        let filenames = get_test_feature_filenames(num_threads);

        let chunk_size = (filenames.len() + num_threads - 1) / num_threads;

        let mut handlers = Vec::new();

        for chunk in filenames.chunks(chunk_size) {
            let chunk_files = chunk.to_vec();

            let reader = LocalFileReader::new(&chunk_files)?;
            let sinker = FeatureSinker::new(table_name, reader, num_rows, num_cols);

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
