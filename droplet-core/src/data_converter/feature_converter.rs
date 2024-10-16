use std::sync::Arc;
use std::sync::RwLock;

use crate::grid_buffer::GridBuffer;
use crate::id_mapping::IDMapping;
use crate::sample_key::SampleKey;
use crate::simple_features::SimpleFeatures;

/// Convert `SimpleFeatures` to `GridSample`.
///
/// Add `SampleKey` and `col_id` for the data.
pub struct SimpleFeaturesConverter {
    /// ID mapping from string to u32.
    id_mapping: Arc<RwLock<IDMapping>>,
}

impl SimpleFeaturesConverter {
    pub fn new(id_mapping: Arc<RwLock<IDMapping>>) -> Self {
        Self { id_mapping }
    }

    /// Convert `SimpleFeatures` to `GridBuffer`.
    ///
    /// The `feature_names` must be provided, and they are converted to ids before creating `GridBuffer`.
    pub fn to_gridbuffer(
        &self,
        simple_features: &SimpleFeatures,
        feature_names: &Vec<String>,
    ) -> Result<GridBuffer> {
        let id_mapping = self.id_mapping.read().unwrap();
        let ids = id_mapping.get_ids(feature_names)?;

        let sample_key_ids = SampleKey::get_sample_key_ids();

        let gridbuffer = GridBuffer::from_simple_features(sample_key_ids, &ids, simple_features);

        Ok(gridbuffer)
    }
}
