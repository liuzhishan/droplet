use anyhow::{bail, Result};
use log::error;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use strum::{Display, EnumCount, EnumDiscriminants, EnumString, FromRepr};

use crate::db::{db::DB, feature::update_feature_infos};
use crate::error_bail;

/// `FeatureInfo` represents the information of a feature.
///
/// All data are considered as `Feature`.
///
/// It's a little confusing that we use the workd `Feature`, but not `Data`.
/// The reason is that when the training starts, the feature data is feed into the model.
/// We call them `Feature`. And the feature data is coming from some processing of other
/// data. They share the same storage structure and data type. The data before processing
/// can also be used as feature for the model.
///
/// So we use `Feature` to represent all data, just for the sake of consistency.
///
/// `FeatureInfo` is used to describe the core information of a feature.
#[derive(Default, Clone, Deserialize, Serialize)]
pub struct FeatureInfo {
    /// Feature name.
    pub feature_name: String,

    /// Data type.
    pub data_type: DataType,
}

impl FeatureInfo {
    /// Create a new `FeatureInfo`.
    pub fn new(feature_name: &str, data_type: DataType) -> Self {
        Self {
            feature_name: feature_name.to_string(),
            data_type,
        }
    }

    /// Parse from a string.
    ///
    /// If the line is comments, or invalid format, return an error.
    ///
    /// Examples:
    ///
    /// feature_name = "ExtractSparse0", data_type = "U64"
    pub fn from_str(s: &str) -> Result<Self> {
        if s.is_empty() {
            error_bail!("Empty feature info str");
        }

        if s.starts_with('#') {
            error_bail!("Commented line in feature config: {}", s);
        }

        let mut feature_info = FeatureInfo::default();

        for part in s.split(',').map(|s| s.trim()) {
            // Parse the data type, may fail if the data type is not supported.
            let kv = part.split('=').map(|s| s.trim()).collect::<Vec<_>>();

            if kv.len() != 2 {
                error_bail!("Invalid feature info str: {}", part);
            }

            if kv[0] == "feature_name" {
                feature_info.feature_name = kv[1].to_string();
            } else if kv[0] == "data_type" {
                let data_type = match DataType::from_str(kv[1]) {
                    Ok(data_type) => data_type,
                    Err(e) => {
                        error_bail!("Invalid data type: {}, part: {}", e, part);
                    }
                };

                feature_info.data_type = data_type;
            } else {
                // Ignore other fields.
            }
        }

        Ok(feature_info)
    }
}

/// Data type.
#[derive(
    Default,
    Clone,
    FromRepr,
    Debug,
    PartialEq,
    EnumCount,
    EnumDiscriminants,
    EnumString,
    Deserialize,
    Serialize,
    Display,
)]
#[repr(u8)]
pub enum DataType {
    /// `u64`
    #[default]
    U64,

    /// `f32`
    F32,

    /// `u64` list
    U64List,

    /// `f32` list
    F32List,
}

/// Feature config in text file format.
///
/// We use a self-defined config format to describe the feature information. Each line
/// represents a feature, and the attributes are separated by comma.
///
/// `#` is used for comments.
///
/// Examples:
///
/// # user_id feature
/// feature_name = "user_id", data_type = "U64"
/// feature_name = "item_id", data_type = "U64"
///
/// Why not using `toml` format? The `toml` format is a good format, but it's not
/// flexible and concise enough. For example, it doesn't support the enum `data_type` field.
/// And one feature each line is more clear and easier to parse.
#[derive(Deserialize, Serialize)]
pub struct FeatureConfig {
    /// All the feature infos.
    feature_infos: Vec<FeatureInfo>,
}

impl FeatureConfig {
    /// Create a new `FeatureConfig`.
    pub fn new(feature_infos: Vec<FeatureInfo>) -> Self {
        Self { feature_infos }
    }

    /// Parse from a text file.
    pub fn from_config_file(filename: &str) -> Result<Self> {
        let content = std::fs::read_to_string(filename)?;
        Self::from_config_str(&content)
    }

    /// Parse from string.
    ///
    /// For each line, parse the required field of `FeatureInfo`.
    pub fn from_config_str(content: &str) -> Result<Self> {
        let feature_infos: Vec<FeatureInfo> = content
            .split('\n')
            .filter(|line| !line.trim().is_empty() && !line.trim().starts_with('#'))
            .filter_map(|line| match FeatureInfo::from_str(line) {
                Ok(feature_info) => Some(feature_info),
                Err(e) => {
                    error!("Invalid feature config: {}, line: {}", e, line);
                    None
                }
            })
            .collect();

        Ok(Self::new(feature_infos))
    }

    /// Write feature info to database.
    pub fn write_to_db(&self, db: &DB) -> Result<()> {
        update_feature_infos(db, &self.feature_infos)
    }
}
