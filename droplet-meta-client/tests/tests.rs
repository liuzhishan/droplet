use anyhow::Result;
use log::info;

use droplet_core::droplet::ColumnInfo;
use droplet_core::{droplet::DataType, tool::setup_log};
use droplet_meta_client::client::MetaClientWrapper;

#[tokio::test]
async fn test_insert_table_info() -> Result<()> {
    setup_log();

    let mut meta_client = MetaClientWrapper::get_default_client().await?;

    let table = "test_grid_sinker";
    let partition_count_per_day = 24 * 12;

    let sparse_count = 76;
    let dense_count = 5;

    let sparse_feature_names = (0..sparse_count)
        .map(|i| format!("ExtractSparse{}", i))
        .collect::<Vec<String>>();

    let sparse_feature_ids = meta_client.get_key_ids(&sparse_feature_names)?;

    let sparse_features = sparse_feature_names
        .iter()
        .enumerate()
        .map(|(i, name)| ColumnInfo {
            column_name: name.clone(),
            column_type: DataType::I32Array.into(),
            column_id: sparse_feature_ids[i],
            column_index: i as u32,
        });

    let dense_feature_names = (0..dense_count)
        .map(|i| format!("ExtractDense{}", i + sparse_count))
        .collect::<Vec<String>>();

    let dense_feature_ids = meta_client.get_key_ids(&dense_feature_names)?;

    let dense_features = dense_feature_names
        .iter()
        .enumerate()
        .map(|(i, name)| ColumnInfo {
            column_name: name.clone(),
            column_type: DataType::F32Array.into(),
            column_id: dense_feature_ids[i],
            column_index: (i + sparse_count) as u32,
        });

    let mut columns = sparse_features.chain(dense_features).collect();

    meta_client.insert_table_info(table, partition_count_per_day, &columns)?;

    Ok(())
}
