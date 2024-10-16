/// Feature related operations.
use anyhow::{bail, Result};
use log::{error, info};

use mysql::prelude::Queryable;

use mysql::params;

use crate::db::db::DB;
use crate::error_bail;
use crate::feature_info::FeatureInfo;
use crate::grid_sample::SampleKey;

/// Update feature info into database.
///
/// If the feature name already exists, check whether the data type is the same.
pub fn update_feature_infos(db: &DB, feature_infos: &[FeatureInfo]) -> Result<()> {
    let mut conn = db.get_conn()?;

    // Check whether the feature name already exists and data type is the same.
    for feature_info in feature_infos {
        let info: Option<(String, u32)> = conn.query_first(format!(
            "select feature_name, data_type from feature_info where feature_name = '{}'",
            feature_info.feature_name.clone()
        ))?;

        if let Some((name, data_type)) = info {
            if data_type != feature_info.data_type.clone() as u32 {
                error_bail!(
                    "data type mismatch for feature: {}, data type in db: {}, data type in config: {}",
                    feature_info.feature_name,
                    data_type,
                    feature_info.data_type.to_string()
                );
            }
        }
    }

    // Insert into `feature_info` table.
    conn.exec_batch(
        "INSERT INTO feature_info (feature_name, data_type) VALUES (:feature_name, :data_type)
         ON DUPLICATE KEY UPDATE data_type = VALUES(data_type)",
        feature_infos
            .iter()
            .map(|fi| params! { "feature_name" => fi.feature_name.clone(), "data_type" => fi.data_type.clone() as u32 }),
    )?;

    // Update `id_mapping` table.
    conn.exec_batch(
        "INSERT INTO id_mapping (name) VALUES (:name)",
        feature_infos
            .iter()
            .map(|fi| params! { "name" => fi.feature_name.clone() }),
    )?;

    Ok(())
}

/// Insert the keys of sample keys into `id_mapping` table.
pub fn insert_sample_keys(db: &DB) -> Result<()> {
    let mut conn = db.get_conn()?;

    let keys = SampleKey::get_sample_key_names();

    conn.exec_batch(
        r#"
        INSERT INTO id_mapping (name)
        SELECT name FROM (SELECT (:name) as name) AS t
        WHERE NOT EXISTS (
            SELECT name FROM id_mapping WHERE name = (:name)
        )
        LIMIT 1
        "#,
        keys.iter()
            .map(|name| params! { "name" => name.to_string() }),
    )?;

    Ok(())
}
