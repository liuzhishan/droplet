use anyhow::Result;
use log::info;

use std::sync::Arc;

use droplet_core::db::db::DB;

use droplet_core::db::meta_info::{get_key_ids, get_table_paths_by_date};

use droplet_core::droplet::meta_client::MetaClient;
use droplet_meta_server::tool::{get_meta_server_client, get_meta_server_default_client};

pub struct MetaClientWrapper {
    db: Arc<DB>,
    client: MetaClient<tonic::transport::Channel>,
}

impl MetaClientWrapper {
    pub async fn new(db: Arc<DB>, meta_server_endpoint: &String) -> Result<Self> {
        let client = get_meta_server_client(meta_server_endpoint).await?;
        Ok(Self { db, client })
    }

    pub async fn get_default_client() -> Result<Self> {
        let db = Arc::new(DB::new()?);
        let client = get_meta_server_default_client().await?;

        Ok(Self { db, client })
    }

    /// Get the paths for a given table and partition date.
    ///
    /// Other method to get paths would be supported in the future.
    pub fn get_paths_by_date(&mut self, table: &str, partition_date: u32) -> Result<Vec<String>> {
        let mut conn = self.db.get_conn()?;

        get_table_paths_by_date(&mut conn, table, partition_date)
    }

    pub fn get_key_ids(&mut self, keys: &Vec<String>) -> Result<Vec<u32>> {
        let mut conn = self.db.get_conn()?;

        get_key_ids(&mut conn, keys)
    }
}
