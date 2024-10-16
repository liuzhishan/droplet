/// Global ID mapping.
///
/// IDMapping use map string to u32 globally. It use auto increment method to generate the id.
/// The result is storing into mysql database of meta server.
use anyhow::{bail, Result};
use dashmap::DashMap;
use mysql::prelude::Queryable;
use mysql::{Pool, PooledConn};

use log::error;

use crate::db::db::DB;
use crate::error_bail;

/// The global ID mapping.
///
/// The mapping is stored in mysql database. `mapping` is a `DashMap` for cache.
/// When we want to get the id of a string, we first search it in `mapping`. If not found,
/// we will get the id from mysql and then store it in `mapping`.
pub struct IDMapping {
    /// The mapping from string to u32.
    mapping: DashMap<String, u32>,

    /// The mysql connection pool.
    db: DB,
}

impl IDMapping {
    /// Create a new IDMapping.
    pub fn new() -> Result<Self> {
        let db = DB::new()?;

        Ok(Self {
            mapping: DashMap::new(),
            db,
        })
    }

    /// Create a new IDMapping with a given `DB` instance.
    ///
    /// Must provide a valid `DB` instance.
    pub fn with_db(db: DB) -> Self {
        Self {
            mapping: DashMap::new(),
            db,
        }
    }

    /// Get the id of a string.
    ///
    /// First look up the id in `mapping`. If not found, then query from mysql.
    /// If still not found, insert the name into mysql and return the new id.
    ///
    /// It will always return a valid id.
    pub fn get_id(&self, name: &String) -> Result<u32> {
        let id_opt = self.mapping.get(name).map(|id| id.value().clone());

        match id_opt {
            Some(id) => Ok(id),
            None => {
                let mut conn = self.db.get_conn()?;

                match self.get_id_from_mysql(name, &mut conn) {
                    Ok(id) => {
                        self.mapping.insert(name.clone(), id);
                        Ok(id)
                    }
                    Err(_) => {
                        let new_id = self.get_new_id(name, &mut conn)?;
                        self.mapping.insert(name.clone(), new_id);
                        Ok(new_id)
                    }
                }
            }
        }
    }

    /// Get ids of a list of names.
    pub fn get_ids(&self, names: &Vec<String>) -> Result<Vec<u32>> {
        let mut ids = Vec::with_capacity(names.len());

        for name in names {
            let id = self.get_id(name)?;
            ids.push(id);
        }

        Ok(ids)
    }

    /// Get id from mysql.
    fn get_id_from_mysql(&self, name: &String, conn: &mut PooledConn) -> Result<u32> {
        let sql = format!("select id from id_mapping where name = '{}'", name);
        let res = conn.query_first::<u32, _>(sql)?;

        match res {
            Some(id) => Ok(id),
            None => Err(anyhow::anyhow!("ID not found in mysql")),
        }
    }

    /// Get a new id for a name.
    ///
    /// Insert the name into mysql and return the new id.
    fn get_new_id(&self, name: &String, conn: &mut PooledConn) -> Result<u32> {
        if self.get_id_from_mysql(name, conn).is_ok() {
            error_bail!("name already exists in mysql, name: {}", name);
        }

        let sql = format!("insert into id_mapping (name) values ('{}')", name);
        conn.query_drop(sql)?;

        self.get_id_from_mysql(name, conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::db::DB;
    use crate::tool::setup_log;
    use log::info;

    #[test]
    fn test_get_id() -> Result<()> {
        setup_log();

        let db = DB::new()?;
        let id_mapping = IDMapping::with_db(db);

        // Test getting an existing ID
        let name = "droplet".to_string();
        let id = id_mapping.get_id(&name)?;
        assert_eq!(id, 1);

        Ok(())
    }

    #[test]
    fn test_get_new_id() -> Result<()> {
        setup_log();

        let id_mapping = IDMapping::new()?;
        let mut conn = id_mapping.db.get_conn()?;

        // Test getting a new ID
        let name = "photo_id".to_string();
        let new_id = 7;

        assert!(id_mapping.get_new_id(&name, &mut conn).is_err());

        // Verify that the new ID is in the database
        let verified_id = id_mapping.get_id_from_mysql(&name, &mut conn)?;
        assert_eq!(new_id, verified_id);

        info!("new_id: {}", new_id);

        Ok(())
    }
}
