use droplet_core::{db::{db::DB, feature::insert_sample_keys}, feature_info::FeatureConfig, local_file_reader::LocalFileReader, tool::setup_log};

use anyhow::Result;

#[test]
fn test_local_file_reader() -> Result<()> {
    setup_log();

    let filenames = vec![
        "resources/test.txt".to_string(),
        "resources/test.txt".to_string(),
    ];

    let reader = LocalFileReader::new(&filenames)?;
    let lines = reader
        .filter(|x| x.is_ok())
        .map(|line| line.unwrap())
        .collect::<Vec<String>>();

    assert_eq!(lines[0], "aaa");
    assert_eq!(lines[1], "bbb");
    assert_eq!(lines[2], "ccc");
    assert_eq!(lines[3], "ddd");
    assert_eq!(lines[4], "eee");
    assert_eq!(lines[5], "fff");
    assert_eq!(lines[6], "ggg");
    assert_eq!(lines[7], "hhh");
    assert_eq!(lines[8], "iii");
    assert_eq!(lines[9], "jjj");

    Ok(())
}

#[test]
fn update_feature_config_to_db() -> Result<()> {
    setup_log();

    let feature_config_path = "resources/dsp_ctr_simple_features.config";
    let feature_config = FeatureConfig::from_config_file(feature_config_path)?;

    let db = DB::new()?;
    feature_config.write_to_db(&db)?;

    Ok(())
}

#[test]
fn test_insert_sample_keys() -> Result<()> {
    setup_log();

    let db = DB::new()?;
    insert_sample_keys(&db)?;

    Ok(())
}
