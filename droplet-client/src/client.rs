use anyhow::{bail, Result};
use droplet_core::droplet::{
    droplet_client::DropletClient, HeartbeatRequest, NodeStatus, SinkGridSampleRequest,
    StartSinkPartitionRequest,
};
use droplet_server::tool::{get_droplet_client, get_droplet_default_client};
use std::iter::Iterator;

use droplet_core::error_bail;
use log::{error, info};
use std::sync::Arc;

use droplet_core::db::db::DB;
use gridbuffer::core::gridbuffer::GridBuffer;

use crate::gridbuffer_reader::{
    GridRowRef, GridRowRefs, LocalGridRowMergeReader, LocalGridRowReader,
};

use super::gridbuffer_reader::{LocalGridBufferMergeReader, LocalGridbufferReader};
use droplet_core::droplet::FinishSinkPartitionRequest;
use droplet_meta_client::client::MetaClientWrapper;

/// Wrapper of grpc droplet client.
///
/// `Client` implements core logic of reading and writing data to droplet server.
pub struct Client {
    droplet_client: DropletClient<tonic::transport::Channel>,
    meta_client: MetaClientWrapper,
}

impl Client {
    pub async fn new(
        server_endpoint: &String,
        db: Arc<DB>,
        meta_server_endpoint: &String,
    ) -> Result<Self> {
        match get_droplet_client(server_endpoint).await {
            Ok(droplet_client) => match MetaClientWrapper::new(db, meta_server_endpoint).await {
                Ok(meta_client) => Ok(Self {
                    droplet_client,
                    meta_client,
                }),
                Err(e) => {
                    error_bail!(
                        "Failed to connect to meta server, endpoint: {}, error: {}",
                        meta_server_endpoint.clone(),
                        e
                    );
                }
            },
            Err(e) => {
                error_bail!(
                    "Failed to connect to droplet server, endpoint: {}, error: {}",
                    server_endpoint.clone(),
                    e
                );
            }
        }
    }

    pub async fn new_client_by_server_endpoint(server_endpoint: &str) -> Result<Self> {
        let droplet_client = get_droplet_client(server_endpoint).await?;
        let meta_client = MetaClientWrapper::get_default_client().await?;

        Ok(Self {
            droplet_client,
            meta_client,
        })
    }

    pub async fn get_default_client() -> Result<Self> {
        match get_droplet_default_client().await {
            Ok(droplet_client) => match MetaClientWrapper::get_default_client().await {
                Ok(meta_client) => Ok(Self {
                    droplet_client,
                    meta_client,
                }),
                Err(e) => {
                    error_bail!("Failed to get default meta server client, error: {}", e);
                }
            },
            Err(e) => {
                error_bail!("Failed to get default droplet client, error: {}", e);
            }
        }
    }

    /// Read gridbuffer from single table.
    ///
    /// Read local files for test.
    pub fn read_gridbuffer(
        &mut self,
        table: &str,
        partition_date: u32,
        keys: &Vec<String>,
    ) -> Result<impl Iterator<Item = GridRowRef>> {
        let file_paths = self.meta_client.get_paths_by_date(table, partition_date)?;
        let key_ids = self.meta_client.get_key_ids(keys)?;

        LocalGridRowReader::new(file_paths, key_ids)
    }

    /// Merge on read.
    pub fn read_gridbuffer_merge(
        &mut self,
        tables: &Vec<String>,
        partition_date: u32,
        keys: &Vec<Vec<String>>,
    ) -> Result<impl Iterator<Item = GridRowRef>> {
        if tables.len() != keys.len() {
            error_bail!(
                "The length of tables and keys must be the same, tables.len(): {}, keys.len(): {}",
                tables.len(),
                keys.len()
            );
        }

        let mut readers = Vec::with_capacity(tables.len());
        let mut key_ids = Vec::with_capacity(keys.len());

        for i in 0..tables.len() {
            let paths = self
                .meta_client
                .get_paths_by_date(&tables[i], partition_date)?;
            let ids = self.meta_client.get_key_ids(&keys[i])?;

            key_ids.push(ids.clone());

            let reader = LocalGridRowReader::new(paths, ids)?;
            readers.push(reader);
        }

        Ok(LocalGridRowMergeReader::new(readers, key_ids))
    }

    pub async fn start_sink_partition(
        &mut self,
        table: &str,
        sinker_id: u32,
        partition_index: u32,
    ) -> Result<()> {
        let path = self.meta_client.get_path_by_table(&table);
        let path_id = self.meta_client.get_or_insert_key_id(path.as_str())?;

        self.droplet_client
            .start_sink_partition(StartSinkPartitionRequest {
                path,
                path_id,
                sinker_id,
                partition_index,
            })
            .await?;

        Ok(())
    }

    pub async fn sink_grid_sample(
        &mut self,
        table: &str,
        path_id: Option<u32>,
        sinker_id: u32,
        partition_index: u32,
        gridbuffer: GridBuffer,
    ) -> Result<()> {
        let new_path_id = path_id.unwrap_or(self.meta_client.get_or_insert_key_id(table)?);

        self.droplet_client
            .sink_grid_sample(SinkGridSampleRequest {
                path_id: new_path_id,
                sinker_id,
                partition_index,
                grid_sample_bytes: gridbuffer.to_bytes(),
            })
            .await?;

        Ok(())
    }

    pub async fn finish_sink_partition(
        &mut self,
        path_id: u32,
        sinker_id: u32,
        partition_index: u32,
    ) -> Result<()> {
        self.droplet_client
            .finish_sink_partition(FinishSinkPartitionRequest {
                path_id,
                sinker_id,
                partition_index,
            })
            .await?;

        Ok(())
    }

    pub async fn heartbeat(&mut self, node_id: u32) -> Result<()> {
        self.droplet_client
            .heartbeat(HeartbeatRequest {
                node_id,
                status: NodeStatus::Alive.into(),
            })
            .await?;

        Ok(())
    }
}
