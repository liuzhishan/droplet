use anyhow::{anyhow, bail, Result};
use gridbuffer::core::gridbuffer::GridBuffer;
use likely_stable::unlikely;
use log::{error, info};
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::error_bail;
use crate::grid_sample::{GridRow, GridRows};
use crate::tool::is_keys_equal;

/// The key type to compare the elements.
pub trait HeapOrderKey {
    type Key: Ord;

    fn key(&self) -> Self::Key;
}

/// A min-heap to maintain the top `window_size` elements.
///
/// To achieve better performance, we need to avoid `malloc` and `copy` as much as possible.
///
/// We use a `Vec` `elements` to store the `GridBuffer`, to avoid `allocation` overhead.
/// When a `GridBuffer` is popped, we just mark the position in the `Vec` as empty.
///
/// The available position is stored in a `Vec` `available_positions` as stack.
///
/// The element in the heap is the row of the element and the index of the element in the `elements`.
///
/// When a `GridBuffer` is pushed into `elements`, and there are availabel positions, we put the
/// `GridBuffer` into the position, and pop `GridRow`s to construct a new `GridBuffer`.
pub struct WindowHeap {
    /// The number of `GridBuffer`.
    window_size: usize,

    /// The max number of elements in the heap.
    heap_size: usize,

    /// The heap to maintain the top `GridRows`s.
    heap: BinaryHeap<Reverse<(GridRow, usize)>>,

    /// A stack to maintain the available positions in the `elements` `Vec`.
    available_positions: Vec<usize>,

    /// The elements in the heap.
    ///
    /// All elements must have same column ids, which means same `cols` in `GridBuffer`.
    elements: Vec<GridBuffer>,

    /// The number of rows in each result `GridBuffer`.
    batch_size: usize,

    /// Record the number of rows left in each `GridBuffer`.
    ///
    /// If the number of rows left is 0, the `GridBuffer` is marked as dropped. So the index of `elements`
    /// is available to be reused.
    num_rows_left: Vec<usize>,

    /// The output `GridBuffer`s, which is the result of merging the `GridRow`s in the heap.
    ///
    /// Why use `Vec` but not a `GridBuffer` ?
    ///
    /// Because if there are no available positions when pushing a new `GridBuffer`, we need to pop `GridRow`s
    /// from the heap until there are available positions. We are not sure how many `GridRow`s will be popped,
    /// they may construct multiple output `GridBuffer`s. So we need use `Vec` to store them.
    out_gridbuffers: Vec<GridBuffer>,

    /// For constructing new `GridBuffer`.
    gridrows: GridRows,

    /// `col_ids` of all `GridBuffer`s.
    col_ids: Vec<u32>,

    /// The hash of `col_ids`.
    col_ids_hash: u32,
}

impl WindowHeap {
    pub fn new(window_size: usize, batch_size: usize) -> Self {
        let total = window_size * batch_size;

        Self {
            window_size,
            heap_size: total,
            heap: BinaryHeap::with_capacity(total),
            available_positions: (0..window_size).rev().collect(),
            elements: Vec::with_capacity(window_size),
            batch_size,
            num_rows_left: vec![0; window_size],
            out_gridbuffers: Vec::with_capacity(20),
            gridrows: GridRows::new(),
            col_ids: Vec::new(),
            col_ids_hash: 0,
        }
    }

    /// Push a new element into the heap.
    ///
    /// The `cols` of all elements must be same.
    ///
    /// If the heap is not full, push the (key, index) into the heap and store the element in the `elements`.
    /// If the heap is full, return error.
    ///
    /// The `num_rows` in input `GridBuffer` and the `batch_size` may not be the same.
    ///
    /// How often should we get the output `GridBuffer` ?
    ///
    /// If the `num_rows` is less than `batch_size`, for example, `num_rows = 4` and `batch_size = 16`, we
    /// would get one output `GridBuffer` after pushing 4 `GridBuffer`s, we need to check output every 4 input.
    ///
    /// If the `num_rows` is greater than `batch_size`, for example, `num_rows = 16` and `batch_size = 4`,
    /// we would get `4` output `GridBuffer`s after pushing `1` `GridBuffer`s, we need to check output every
    /// intput.
    ///
    /// One easy way is to check length of `out_gridbuffers`. If the length is greater than `0`, we pop
    /// all the outputs. This could be done outside of the window heap.
    pub fn push(&mut self, gridbuffer: GridBuffer) -> Result<()> {
        if self.col_ids.is_empty() {
            self.col_ids = gridbuffer.col_ids().clone();
            self.col_ids_hash = gridbuffer.col_ids_hash();
        }

        if unlikely(self.col_ids_hash != gridbuffer.col_ids_hash()) {
            error_bail!(
                "The col_ids is not same! self.col_ids: {:?}, self.col_ids_hash: {:?}, gridbuffer.col_ids: {:?}, gridbuffer.col_ids_hash: {:?}",
                self.col_ids,
                self.col_ids_hash,
                gridbuffer.col_ids().clone(),
                gridbuffer.col_ids_hash()
            );
        }

        match self.available_positions.pop() {
            Some(index) => {
                if index < self.elements.len() {
                    self.elements[index] = gridbuffer;
                } else if index == self.elements.len() {
                    self.elements.push(gridbuffer);
                } else {
                    error_bail!(
                        "The index is out of bounds, index: {}, elements: {}",
                        index,
                        self.elements.len()
                    );
                }

                self.num_rows_left[index] = self.elements[index].num_rows();

                for i in 0..self.elements[index].num_rows() {
                    let row = GridRow::new(&self.elements[index], i);
                    self.heap.push(Reverse((row, index)));
                }

                Ok(())
            }
            None => {
                // The `elements` is full, we need to pop some `GridRow`s to make space for the new `GridBuffer`.
                while let Some(Reverse((gridrow, index))) = self.heap.pop() {
                    self.gridrows.push(gridrow);

                    // Must convert to `GridBuffer` before drop the element, because the `GridRow` contains the
                    // pointer of `GridBuffer`.
                    if self.gridrows.len() >= self.batch_size {
                        self.out_gridbuffers.push(self.gridrows.to_gridbuffer());
                        self.gridrows.clear();
                    }

                    // For safety, unlikely to happen.
                    if unlikely(self.num_rows_left[index] == 0) {
                        self.elements[index] = gridbuffer;
                        self.num_rows_left[index] = self.elements[index].num_rows();
                        break;
                    }

                    self.num_rows_left[index] -= 1;
                    if self.num_rows_left[index] == 0 {
                        self.elements[index] = gridbuffer;
                        self.num_rows_left[index] = self.elements[index].num_rows();
                        break;
                    }
                }

                Ok(())
            }
        }
    }

    pub fn get_out_gridbuffer(&mut self) -> Option<GridBuffer> {
        self.out_gridbuffers.pop()
    }

    pub fn out_gridbuffers(&self) -> &[GridBuffer] {
        &self.out_gridbuffers
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Check if the window heap is full.
    pub fn is_full(&self) -> bool {
        self.heap.len() == self.window_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id_mapping::IDMapping;
    use crate::{grid_sample::SampleKey, tool::setup_log};
    use anyhow::Result;
    use gridbuffer::core::gridbuffer::GridBuffer;

    fn create_test_gridbuffer(num_rows: usize) -> Result<GridBuffer> {
        setup_log();

        let id_mapping = IDMapping::new()?;

        let feature_names = vec![
            "ExtractSparse0".to_string(),
            "ExtractSparse1".to_string(),
            "ExtractSparse2".to_string(),
            "ExtractSparse3".to_string(),
        ];

        let feature_ids = id_mapping.get_ids(&feature_names)?;

        let col_ids = SampleKey::get_sample_key_ids()
            .iter()
            .chain(feature_ids.iter())
            .map(|id| *id)
            .collect();

        let mut gb = GridBuffer::new_with_num_rows_col_ids(num_rows, col_ids);

        for i in 0..num_rows {
            gb.push_u64(i, 0, i as u64);
            gb.push_u64(i, 1, (i + 1) as u64);
            gb.push_u64(i, 2, (i + 2) as u64);
            gb.push_u64(i, 3, (i + 3) as u64);

            gb.push_u64(i, 4, 10 as u64);
            gb.push_u64(i, 5, 11 as u64);
            gb.push_u64(i, 6, 12 as u64);
            gb.push_u64(i, 7, 13 as u64);
        }

        Ok(gb)
    }

    #[test]
    fn test_window_heap_push() -> Result<()> {
        let mut heap = WindowHeap::new(3, 4);

        // Push 3 GridBuffers (window size)
        heap.push(create_test_gridbuffer(2)?)?;
        heap.push(create_test_gridbuffer(2)?)?;
        heap.push(create_test_gridbuffer(2)?)?;

        assert!(!heap.is_full());
        assert_eq!(heap.len(), 6); // 3 GridBuffers * 2 rows each

        // Push another GridBuffer, should trigger output
        heap.push(create_test_gridbuffer(2)?)?;

        assert_eq!(heap.out_gridbuffers().len(), 1);
        let out_gb = heap.get_out_gridbuffer().unwrap();
        assert_eq!(out_gb.num_rows(), 4); // batch size

        info!("out gridbuffer bypes: {:?}", out_gb.estimated_bytes());

        Ok(())
    }

    #[test]
    fn test_window_heap_order() -> Result<()> {
        let mut heap = WindowHeap::new(3, 4);

        heap.push(create_test_gridbuffer(2)?)?;
        heap.push(create_test_gridbuffer(2)?)?;
        heap.push(create_test_gridbuffer(2)?)?;

        // Push to trigger output
        heap.push(create_test_gridbuffer(2)?)?;

        let out_gb = heap.get_out_gridbuffer().unwrap();
        assert_eq!(out_gb.num_rows(), 4);

        // Check if the output is in correct order
        let timestamps: Vec<u64> = (0..4).filter_map(|i| out_gb.get_u64(i, 0)).collect();
        assert_eq!(timestamps, vec![0, 0, 0, 1]);

        info!("out gridbuffer bypes: {:?}", out_gb.estimated_bytes());

        Ok(())
    }

    #[test]
    fn test_window_heap_overflow() -> Result<()> {
        let mut heap = WindowHeap::new(2, 4);

        heap.push(create_test_gridbuffer(2)?)?;
        heap.push(create_test_gridbuffer(2)?)?;

        assert!(!heap.is_full());

        // This should trigger overflow handling
        heap.push(create_test_gridbuffer(2)?)?;
        heap.push(create_test_gridbuffer(2)?)?;

        assert_eq!(heap.out_gridbuffers().len(), 1);

        let out_gb = heap.get_out_gridbuffer();

        assert!(out_gb.is_some());

        let timestamp = out_gb.unwrap().get_u64(0, 0).unwrap();
        assert_eq!(timestamp, 0);

        Ok(())
    }
}
