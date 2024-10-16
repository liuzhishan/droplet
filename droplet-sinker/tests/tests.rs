use anyhow::Result;
use log::info;

use droplet_core::{local_file_reader::LocalFileReader, tool::setup_log};
use droplet_sinker::feature_sinker::FeatureSinker;
use droplet_sinker::grid_sinker::GridSinker;

#[tokio::test]
async fn test_feature_sinker() -> Result<()> {
    setup_log();

    let num_rows = 16;
    let num_cols = 81;

    let num_threads = 2;

    FeatureSinker::<LocalFileReader>::start_local_file_sinker(
        &"test_feature_sinker".to_string(),
        num_rows,
        num_cols,
        num_threads,
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_grid_sinker() -> Result<()> {
    setup_log();

    let num_threads = 2;

    GridSinker::<LocalFileReader>::start_local_file_sinker(
        &"test_grid_sinker".to_string(),
        num_threads,
    )
    .await?;

    Ok(())
}
