use anyhow::bail;
use anyhow::Result;
use chrono::NaiveDateTime;
use chrono::Timelike;
use gethostname::gethostname;
use log::{error, info};
use std::time::Duration;
use tokio::task;
use tokio_graceful_shutdown::SubsystemBuilder;
use tokio_graceful_shutdown::Toplevel;

use std::sync::Arc;
use std::sync::RwLock;

use tokio_graceful_shutdown::SubsystemHandle;

use gridbuffer::core::gridbuffer::GridBuffer;

use droplet_core::grid_sample::GridRow;
use droplet_core::grid_sample::GridSample;

use likely_stable::likely;

use droplet_client::client::Client;
use droplet_core::db::db::DB;
use droplet_core::error_bail;
use droplet_core::id_mapping::IDMapping;
use droplet_core::local_file_reader::{get_test_gridbuffer_filenames, LocalFileReader};
use droplet_core::window_heap::WindowHeap;
use droplet_meta_client::client::MetaClientWrapper;

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

    /// Meta client to get meta information.
    meta_client: MetaClientWrapper,

    /// The number of partitions per day.
    partition_count_per_day: u32,

    /// The ID of the sinker.
    sinker_id: u32,

    /// The name of the path.
    path: String,

    /// The ID of the path.
    path_id: u32,
}

impl<T: Iterator<Item = Result<String>>> GridSinker<T> {
    /// Create a new GridSinker instance using a BufRead.
    pub fn new(
        table_name: &str,
        reader: T,
        id_mapping: Arc<RwLock<IDMapping>>,
        mut meta_client: MetaClientWrapper,
    ) -> Result<Self> {
        let batch_size = 4;
        let partition_count_per_day = meta_client.get_partition_count_per_day(table_name)?;

        let hostname = gethostname();
        let sinker_id = meta_client.get_or_insert_key_id(&hostname.to_string_lossy())?;

        let path = meta_client.get_path_by_table(&table_name);
        let path_id = meta_client.get_or_insert_key_id(path.as_str())?;

        Ok(Self {
            table_name: table_name.to_string(),
            reader,
            window_heap: WindowHeap::new(2, batch_size),
            id_mapping,
            meta_client,
            partition_count_per_day,
            sinker_id,
            path,
            path_id,
        })
    }

    fn get_partition_index_by_timestamp(
        gridbuffer: &GridBuffer,
        partition_count_per_day: u32,
    ) -> Result<u32> {
        if gridbuffer.num_rows() == 0 {
            bail!("Gridbuffer is empty");
        }

        let row = GridRow::new(gridbuffer, 0);
        let timestamp = row.get_sample_key().timestamp;

        let naive_datetime = NaiveDateTime::from_timestamp_opt(timestamp as i64, 0)
            .ok_or_else(|| anyhow::anyhow!(format!("Invalid timestamp: {}", timestamp)))?;
        let seconds_in_day = naive_datetime.num_seconds_from_midnight();

        let time_span_in_seconds: i64 = 86400 / partition_count_per_day as i64;
        let partition_index = seconds_in_day / time_span_in_seconds as u32;

        Ok(partition_index)
    }

    async fn get_droplet_client_by_partition_index(
        &mut self,
        partition_index: u32,
    ) -> Result<Client> {
        let server_endpoint = self
            .meta_client
            .get_server_endpoint_by_partition_index(&self.table_name, partition_index)?;
        let client = Client::new_client_by_server_endpoint(&server_endpoint).await?;

        Ok(client)
    }

    /// Start the GridSinker process.
    pub async fn run(mut self, subsys: SubsystemHandle) -> Result<()> {
        let mut gridbuffers = self.reader.filter_map(|line| match line {
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

        let first_gridbuffer = match gridbuffers.next() {
            Some(gridbuffer) => gridbuffer,
            None => {
                error_bail!("No gridbuffer found");
            }
        };

        let mut partition_index: u32 = match Self::get_partition_index_by_timestamp(
            &first_gridbuffer,
            self.partition_count_per_day,
        ) {
            Ok(partition_index) => partition_index,
            Err(e) => {
                error_bail!("Failed to get partition index, error: {}", e);
            }
        };

        let mut server_endpoint = self.meta_client.get_default_server_endpoint();

        let mut client = match Client::new_client_by_server_endpoint(&server_endpoint).await {
            Ok(client) => client,
            Err(e) => {
                error_bail!("Failed to new client, error: {}", e);
            }
        };

        match client.heartbeat(self.sinker_id).await {
            Ok(_) => (),
            Err(e) => {
                error_bail!("Failed to heartbeat, error: {}", e);
            }
        };

        match client
            .start_sink_partition(&self.table_name, self.sinker_id, partition_index)
            .await
        {
            Ok(_) => (),
            Err(e) => {
                error_bail!("Failed to start sink partition, error: {}", e);
            }
        };

        client
            .sink_grid_sample(
                &self.table_name,
                Some(self.path_id),
                self.sinker_id,
                partition_index,
                first_gridbuffer,
            )
            .await?;

        for gridbuffer in gridbuffers {
            self.window_heap.push(gridbuffer)?;

            if self.window_heap.out_gridbuffers().len() > 0 {
                while let Some(gridbuffer) = self.window_heap.get_out_gridbuffer() {
                    if likely(gridbuffer.num_rows() > 0) {
                        let current_partition_index = Self::get_partition_index_by_timestamp(
                            &gridbuffer,
                            self.partition_count_per_day,
                        )?;

                        // If the partition index is changed, we need to switch to the new server endpoint.
                        if current_partition_index != partition_index {
                            client
                                .finish_sink_partition(
                                    self.path_id,
                                    self.sinker_id,
                                    partition_index,
                                )
                                .await?;

                            let new_server_endpoint =
                                self.meta_client.get_default_server_endpoint();

                            if new_server_endpoint != server_endpoint {
                                client =
                                    Client::new_client_by_server_endpoint(&new_server_endpoint)
                                        .await?;
                                server_endpoint = new_server_endpoint;
                            }

                            client
                                .start_sink_partition(
                                    &self.table_name,
                                    self.sinker_id,
                                    current_partition_index,
                                )
                                .await?;
                        }

                        partition_index = current_partition_index;
                        client
                            .sink_grid_sample(
                                &self.table_name,
                                Some(self.path_id),
                                self.sinker_id,
                                partition_index,
                                gridbuffer,
                            )
                            .await?;
                    }
                }
            }
        }

        client
            .finish_sink_partition(self.path_id, self.sinker_id, partition_index)
            .await?;
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

            let mut meta_client = MetaClientWrapper::get_default_client().await?;
            let sinker = GridSinker::new(table_name, reader, id_mapping.clone(), meta_client)?;

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
