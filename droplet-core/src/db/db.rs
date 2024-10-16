/// `mysql` database operations.
use anyhow::{anyhow, bail, Result};
use log::{error, info};

use mysql::prelude::Queryable;
use mysql::*;

#[derive(Clone)]
pub struct DB {
    pool: Pool,
}

impl DB {
    pub fn new() -> Result<Self> {
        let pool = Self::get_connection_pool()?;

        Ok(Self { pool })
    }

    /// TODO: Change to actual db in the future.
    pub fn get_connection_pool() -> Result<Pool> {
        let opts = OptsBuilder::new()
            .user(Some("root"))
            .pass(Some("root"))
            .db_name(Some("droplet"));

        Pool::new(opts).map_err(|e| e.into())
    }

    /// Get a connection from the pool.
    #[inline]
    pub fn get_conn(&self) -> Result<PooledConn> {
        self.pool.get_conn().map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::tool::setup_log;

    use super::*;

    #[test]
    fn test_new_db() {
        let db = DB::new();
        assert!(db.is_ok());
    }

    #[test]
    fn test_get_connection_pool() {
        let pool = DB::get_connection_pool();
        assert!(pool.is_ok());
    }

    #[test]
    fn test_get_conn() {
        let db = DB::new().unwrap();
        let conn = db.get_conn();
        assert!(conn.is_ok());
    }

    #[test]
    fn test_connection_to_database() -> Result<()> {
        setup_log();

        let db = DB::new()?;
        let mut conn = db.get_conn()?;

        // Try to execute a simple query
        let result = conn.query_first::<String, _>("SHOW TABLES")?;
        assert!(result.is_some());

        info!("show tables: {:?}", result);

        Ok(())
    }

    #[test]
    fn test_query() -> Result<()> {
        setup_log();

        let db = DB::new()?;
        let mut conn = db.get_conn()?;

        let sql = "select name, id from id_mapping limit 10";

        let result = conn.query_map(sql, |row: (String, u32)| (row.0, row.1))?;

        for row in result {
            info!("row: {:?}", row);
        }

        Ok(())
    }
}
