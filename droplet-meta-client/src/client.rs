use anyhow::Result;
use droplet_core::droplet::ColumnInfo;
use gethostname::gethostname;
use log::info;

use std::sync::Arc;

use droplet_core::db::db::DB;

use droplet_core::db::meta_info::{
    get_key_ids, get_or_insert_key_id, get_partition_count_per_day,
    get_server_endpoint_by_partition_index, get_table_paths_by_date, insert_table_info,
    is_table_exist,
};

use droplet_core::droplet::meta_client::MetaClient;
use droplet_meta_server::tool::META_SERVER_PORT;
use droplet_meta_server::tool::{get_meta_server_client, get_meta_server_default_client};
use droplet_server::tool::DROPPLET_SERVER_PORT;

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

    pub fn get_or_insert_key_id(&mut self, key: &str) -> Result<u32> {
        let mut conn = self.db.get_conn()?;
        Ok(get_or_insert_key_id(&mut conn, key))
    }

    pub fn get_key_ids(&mut self, keys: &Vec<String>) -> Result<Vec<u32>> {
        let mut conn = self.db.get_conn()?;

        get_key_ids(&mut conn, keys)
    }

    pub fn get_partition_count_per_day(&mut self, table: &str) -> Result<u32> {
        let mut conn = self.db.get_conn()?;

        get_partition_count_per_day(&mut conn, table)
    }

    pub fn get_server_endpoint_by_partition_index(
        &mut self,
        table: &str,
        partition_index: u32,
    ) -> Result<String> {
        let mut conn = self.db.get_conn()?;

        get_server_endpoint_by_partition_index(&mut conn, table, partition_index)
    }

    /// Use local as the default server endpoint.
    pub fn get_default_server_endpoint(&mut self) -> String {
        let hostname = gethostname();
        format!("{}:{}", hostname.to_string_lossy(), DROPPLET_SERVER_PORT)
    }

    pub fn get_path_by_table(&mut self, table: &str) -> String {
        format!("/tmp/droplet/tables/{}", table)
    }

    pub fn is_table_exist(&mut self, table: &str) -> Result<bool> {
        let mut conn = self.db.get_conn()?;

        is_table_exist(&mut conn, table)
    }

    pub fn insert_table_info(
        &mut self,
        table: &str,
        partition_count_per_day: u32,
        columns: &Vec<ColumnInfo>,
    ) -> Result<()> {
        let mut conn = self.db.get_conn()?;

        insert_table_info(&mut conn, table, partition_count_per_day, columns)
    }
}
