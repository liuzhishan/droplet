use std::sync::Arc;
use std::sync::RwLock;

use crate::grid_buffer::GridBuffer;
use crate::id_mapping::IDMapping;
use crate::sample_key::SampleKey;

/// Assemble `GridSample` from `GridBuffer` with flexible number of rows.
///
/// The number of rows in the result `GridSample` is not fixed, but has a minimum number of rows.
/// The input `GridSample` are combined together.
/// 
/// `FlexibleGridAssember` is used for storing data to file.
pub struct FlexibleGridAssembler {
    /// ID mapping from string to u32.
    id_mapping: Arc<RwLock<IDMapping>>,
}
