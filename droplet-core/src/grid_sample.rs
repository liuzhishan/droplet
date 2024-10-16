use anyhow::{anyhow, Result, bail};
use dashmap::DashMap;
use likely_stable::{likely, unlikely};
use log::{error, info};
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};

use gridbuffer::core::gridbuffer::{GridBuffer, GridCell, GridCellU64};

use crate::error_bail;
use crate::tool::is_keys_equal;
use crate::window_heap::HeapOrderKey;

/// Use `timestamp`, `user_id`, `item_id`, `request_id` as the key of the sample.
///
/// For easy of use, the fields are fixed, and the global ids are fixed too. So
/// we write them fixed in code. We first insert the fields into `id_mapping` table,
/// and then get the ids from the table.
/// 
/// Notice: for historical reason, the name of `request_id` is `llsid` in `SimpleFeatures` proto.
#[derive(Eq)]
pub struct SampleKey {
    pub timestamp: u64,
    pub user_id: u64,
    pub item_id: u64,
    pub request_id: u64,
}

impl SampleKey {
    pub fn new(timestamp: u64, user_id: u64, item_id: u64, request_id: u64) -> Self {
        Self {
            timestamp,
            user_id,
            item_id,
            request_id,
        }
    }

    pub fn to_string(&self) -> String {
        format!(
            "{}_{}_{}_{}",
            self.timestamp, self.user_id, self.item_id, self.request_id
        )
    }

    /// For easy of use, the ids of sample keys are fixed.
    /// 
    /// Get directly from the `id_mapping` table.
    #[inline]
    pub fn get_sample_key_ids() -> &'static [u32] {
        &[2, 4, 5, 6]
    }

    #[inline]
    pub fn get_sample_key_names() -> &'static [&'static str] {
        &["timestamp", "user_id", "item_id", "request_id"]
    }

    #[inline]
    pub fn get_sample_key_pair() -> &'static [(&'static str, u32)] {
        &[
            ("timestamp", 2),
            ("user_id", 4),
            ("item_id", 5),
            ("request_id", 6),
        ]
    }

    pub fn is_sample_key_ids(col_ids: &[u32]) -> bool {
        is_keys_equal(SampleKey::get_sample_key_ids(), col_ids)
    }
}

impl PartialEq for SampleKey {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
            && self.user_id == other.user_id
            && self.item_id == other.item_id
            && self.request_id == other.request_id
    }
}

impl Ord for SampleKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.user_id.cmp(&other.user_id))
            .then_with(|| self.item_id.cmp(&other.item_id))
            .then_with(|| self.request_id.cmp(&other.request_id))
    }
}

impl PartialOrd for SampleKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// `GridRow` is a pointer to a row in a `GridBuffer`.
/// 
/// It encapsulates a `GridRow` and provide `SampleKey` for easy access.
pub struct GridRow {
    /// Reference to the `GridBuffer`.
    gridbuffer_ptr: *const GridBuffer,

    /// The index of the row.
    row: usize,
}

unsafe impl Sync for GridRow {}
unsafe impl Send for GridRow {}

impl HeapOrderKey for GridRow {
    type Key = SampleKey;

    fn key(&self) -> Self::Key {
        self.get_sample_key()
    }
}

impl PartialEq for GridRow {
    fn eq(&self, other: &Self) -> bool {
        self.get_sample_key() == other.get_sample_key()
    }
}

impl Eq for GridRow {}

impl Ord for GridRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get_sample_key().cmp(&other.get_sample_key())
    }
}

impl PartialOrd for GridRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl GridRow {
    #[inline]
    pub fn new(gridbuffer_ptr: *const GridBuffer, row: usize) -> Self {
        Self { gridbuffer_ptr, row }
    }

    #[inline]
    pub fn get_gridbuffer(&self) -> &GridBuffer {
        unsafe { &*self.gridbuffer_ptr }
    }

    /// The first four columns must be the sample key ids.
    pub fn is_valid_sample(&self) -> bool {
        let gridbuffer = self.get_gridbuffer();

        if unlikely(gridbuffer.num_cols() < 4) {
            return false;
        }

        SampleKey::is_sample_key_ids(&gridbuffer.col_ids()[0..4])
    }

    pub fn get_sample_key(&self) -> SampleKey {
        let gridbuffer = self.get_gridbuffer();

        SampleKey::new(
            gridbuffer.get_u64(self.row, 0).unwrap_or(0),
            gridbuffer.get_u64(self.row, 1).unwrap_or(0),
            gridbuffer.get_u64(self.row, 2).unwrap_or(0),
            gridbuffer.get_u64(self.row, 3).unwrap_or(0),
        )
    }

    #[inline]
    pub fn get_u64_values(&self, col: usize) -> &[u64] {
        let gridbuffer = self.get_gridbuffer();
        gridbuffer.get_u64_values(self.row, col)
    }

    #[inline]
    pub fn get_f32_values(&self, col: usize) -> &[f32] {
        let gridbuffer = self.get_gridbuffer();
        gridbuffer.get_f32_values(self.row, col)
    }

    #[inline]
    pub fn get_u64(&self, col: usize) -> Option<u64> {
        let gridbuffer = self.get_gridbuffer();
        gridbuffer.get_u64(self.row, col)
    }

    #[inline]
    pub fn get_f32(&self, col: usize) -> Option<f32> {
        let gridbuffer = self.get_gridbuffer();
        gridbuffer.get_f32(self.row, col)
    }

    #[inline]
    pub fn get_cell(&self, col: usize) -> Option<&GridCell> {
        let gridbuffer = self.get_gridbuffer();
        gridbuffer.get_cell(self.row, col)
    }
}

/// A collection of `SampleRow`s.
///
/// It is used to avoid copying data of `GridBuffer`.
///
/// For performance reasons, `GridRows` can be converted to `GridSample` or serialized
/// to string directly, without converting to `GridBuffer`.
pub struct GridRows {
    pub rows: Vec<GridRow>,
}

impl GridRows {
    pub fn new() -> Self {
        Self { rows: vec![] }
    }

    pub fn is_valid_sample(&self) -> bool {
        self.rows.iter().all(|row| row.is_valid_sample())
    }

    pub fn push(&mut self, row: GridRow) {
        self.rows.push(row);
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn clear(&mut self) {
        self.rows.clear();
    }

    /// Assume the `cols` are all same for all rows.
    pub fn to_gridbuffer(&self) -> GridBuffer {
        if self.rows.is_empty() {
            return GridBuffer::new();
        }

        let first_row = &self.rows[0];
        let num_cols = first_row.get_gridbuffer().num_cols();

        let mut gridbuffer = GridBuffer::new_with_num_rows_col_ids_hash(
            self.rows.len(),
            first_row.get_gridbuffer().col_ids().clone(),
            first_row.get_gridbuffer().col_ids_hash(),
        );

        for (i, row) in self.rows.iter().enumerate() {
            for j in 0..num_cols {
                let row_index = row.row;

                // Be careful, we must use `push_u64_values`, cannot use `push_cell`, because the data is in `u64_values` or `f32_values`,
                // the `cell` just contains the index.
                match row.get_cell(j) {
                    Some(cell) => {
                        match cell {
                            GridCell::U64Cell(cell) => {
                                gridbuffer.push_u64_values(i, j, row.get_gridbuffer().get_u64_values(row_index, j));
                            }
                            GridCell::F32Cell(cell) => {
                                gridbuffer.push_f32_values(i, j, row.get_gridbuffer().get_f32_values(row_index, j));
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }

        gridbuffer
    }
}

/// GridSample encapsulates `GridBuffer` with some important information of data.
///
/// It contains the following parts
/// 1. keys: The column of the data.
/// 2. grid_buffer: The underlying data structure to store the data.
///
/// Each row in the grid buffer must have the following key information:
/// 1. timestamp.
/// 2. user_id.
/// 3. item_id.
/// 4. request_id.
///
/// The postion of key information in the row is fixed, they are stored in the first 4 columns
/// of each row in the `GridBuffer`.
pub struct GridSample {
    /// The underlying data structure to store the data.
    pub gridbuffer: GridBuffer,
}

impl GridSample {
    /// Construct a new `GridSample` with the given keys.
    /// 
    /// The sample key ids are fixed and added as the first four columns automatically.
    pub fn new(num_rows: usize, cols: &Vec<u32>) -> Self {
        let all_cols = SampleKey::get_sample_key_ids()
            .iter()
            .chain(cols.iter())
            .cloned()
            .collect();

        let gridbuffer = GridBuffer::new_with_num_rows_col_ids(num_rows, all_cols);

        Self { gridbuffer }
    }

    /// Construct a new `GridSample` from a `GridBuffer`.
    /// 
    /// The first four columns of `gridbuffer` must be the sample key ids.
    pub fn from_gridbuffer(gridbuffer: GridBuffer) -> Result<Self> {
        if unlikely(!SampleKey::is_sample_key_ids(gridbuffer.col_ids())) {
            error_bail!("Invalid gridbuffer, first four columns are not sample key ids");
        }

        Ok(Self { gridbuffer })
    }

    /// Set the sample key of the row.
    #[inline]
    pub fn set_sample_key(&mut self, row: usize, sample_key: &SampleKey) {
        self.gridbuffer.push_u64(row, 0, sample_key.timestamp);
        self.gridbuffer.push_u64(row, 1, sample_key.user_id);
        self.gridbuffer.push_u64(row, 2, sample_key.item_id);
        self.gridbuffer.push_u64(row, 3, sample_key.request_id);
    }

    /// Get the sample key of the row.
    #[inline]
    pub fn get_sample_key(&self, row: usize) -> SampleKey {
        SampleKey::new(
            self.gridbuffer.get_u64(row, 0).unwrap_or(0),
            self.gridbuffer.get_u64(row, 1).unwrap_or(0),
            self.gridbuffer.get_u64(row, 2).unwrap_or(0),
            self.gridbuffer.get_u64(row, 3).unwrap_or(0),
        )
    }

    /// Get timestamp of the row.
    #[inline]
    pub fn get_timestamp(&self, row: usize) -> u64 {
        match self.gridbuffer.get_u64(row, 0) {
            Some(timestamp) => timestamp,
            None => 0,
        }
    }

    /// Get user_id of the row.
    #[inline]
    pub fn get_user_id(&self, row: usize) -> u64 {
        match self.gridbuffer.get_u64(row, 1) {
            Some(user_id) => user_id,
            None => 0,
        }
    }

    /// Get item_id of the row.
    #[inline]
    pub fn get_item_id(&self, row: usize) -> u64 {
        match self.gridbuffer.get_u64(row, 2) {
            Some(item_id) => item_id,
            None => 0,
        }
    }

    /// Get request_id of the row.
    #[inline]
    pub fn get_request_id(&self, row: usize) -> u64 {
        match self.gridbuffer.get_u64(row, 3) {
            Some(request_id) => request_id,
            None => 0,
        }
    }

    /// Insert u64 value into the grid buffer.
    ///
    /// If the key is not found, return an error.
    #[inline]
    pub fn push_u64_values_by_col_id(
        &mut self,
        row: usize,
        col_id: u32,
        values: &[u64],
    ) -> Result<()> {
        self.gridbuffer
            .push_u64_values_by_col_id(row, col_id, values)
    }

    #[inline]
    pub fn push_u64_values(&mut self, row: usize, col: usize, values: &[u64]) {
        self.gridbuffer.push_u64_values(row, col, values)
    }

    /// Insert a u64 value into the grid buffer.
    #[inline]
    pub fn push_u64_by_col_id(&mut self, row: usize, col_id: u32, value: u64) -> Result<()> {
        self.gridbuffer.push_u64_by_col_id(row, col_id, value)
    }

    #[inline]
    pub fn push_u64(&mut self, row: usize, col: usize, value: u64) {
        self.gridbuffer.push_u64(row, col, value)
    }

    /// Insert f32 values into the grid buffer.
    #[inline]
    pub fn push_f32_values_by_col_id(
        &mut self,
        row: usize,
        col_id: u32,
        values: &[f32],
    ) -> Result<()> {
        self.gridbuffer
            .push_f32_values_by_col_id(row, col_id, values)
    }

    #[inline]
    pub fn push_f32_values(&mut self, row: usize, col: usize, values: &[f32]) {
        self.gridbuffer.push_f32_values(row, col, values)
    }

    /// Insert f32 value into the grid buffer.
    #[inline]
    pub fn push_f32_by_col_id(&mut self, row: usize, col_id: u32, value: f32) -> Result<()> {
        self.gridbuffer.push_f32_by_col_id(row, col_id, value)
    }

    #[inline]
    pub fn push_f32(&mut self, row: usize, col: usize, value: f32) {
        self.gridbuffer.push_f32(row, col, value)
    }

    /// Sort the rows by `SampleKey`.
    pub fn sort_rows_by_sample_key(&mut self) {
        let u64_ptr = self.gridbuffer.u64_values().as_ptr();
        let u64_len = self.gridbuffer.u64_values().len();

        self.gridbuffer.sort_rows_by(|k: &Vec<GridCell>| {
            if unlikely(k.len() < 4) {
                error!("Invalid sample key, len: {:?}", k.len());
                return SampleKey::new(0, 0, 0, 0);
            }

            let timestamp = k[0].get_u64(u64_ptr, u64_len);
            let user_id = k[1].get_u64(u64_ptr, u64_len);
            let item_id = k[2].get_u64(u64_ptr, u64_len);
            let request_id = k[3].get_u64(u64_ptr, u64_len);

            match (timestamp, user_id, item_id, request_id) {
                (Some(timestamp), Some(user_id), Some(item_id), Some(request_id)) => {
                    SampleKey::new(timestamp, user_id, item_id, request_id)
                }
                _ => {
                    error!("Invalid sample key");
                    SampleKey::new(0, 0, 0, 0)
                }
            }
        });
    }
}