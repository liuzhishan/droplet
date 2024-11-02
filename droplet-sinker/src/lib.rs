//! Sinker is a component of Droplet that is responsible for converting data into
//! `GridBuffer` format and send to `worker` node.
//!
//! The data is received from log server, in `GridBuffer` format. It may contains one row or
//! many rows, representing one or multiple `items` with same `user` and `context`.
//!
//! The data need to be sorted globally when storing to file eventually at the worker node.
//!
//! The `timestamp` is ordered naturally, but it's not unique for each row. Instead, we use
//! (`timestamp`, `user_id`, `item_id`, `request_id`) as the `SampleKey` for each row, which
//! is unique globally. We construct the global order by the `SampleKey`.
//!
//! For each data, sinker will determine which `worker` node to send based on the meta info.
//!
//! So one of sinker's main functionality is to sort the rows by `SampleKey` when inserting to
//! `GridBuffer`. We need to sort the row by `SampleKey` as early as possible to avoid unnecessary
//! computation at later stage.
//!
//! Is rows in one `GridBuffer` data received from log server sorted by `SampleKey` already?
//!
//! Yes, it is. One `GridBuffer` data from log server is from one `user` and `context`.
//! They are coming from one request, so the time is same, so as `user_id` and `request_id`.
//! `item_id` can be sorted too.
//!
//! Is multiple `GridBuffer` data received from log server sorted by `SampleKey` already?
//!
//! Maybe not. The log server have many workers, and each worker have many threads. When we
//! receive data from log server, it may come from different worker and different threads.
//! The time may be very close for the data, but it can't guarantee the data is sorted by time.
//!
//! How do we sort the data in `Sinker` before inserted to `gridbuffer`? And with high performance?
//!
//! If there are only one row in one `GridBuffer`, the rows in a small time window is generally sorted
//! by time.
//!
//! If there are multiple rows in one `GridBuffer`, for two `GridBuffer` `a` and `b`, there are three
//! cases:
//! 1. All rows in `a` is smaller than rows in `b`.
//! 2. All rows in `a` is greater than rows in `b`.
//! 3. There are rows in `a` and `b` mixed together.
//!
//! By smaller we mean that compared by `SampleKey`. The first two case is easy. But the third case is
//! not easy to handle.
//!
//! We need to sort rows from different `GridBuffer`s. To avoid copying data, we can use reference to
//! represent each row. The final result will be send to other thread through channel, which is not
//! thread safe. So we need to copy the data to a new `GridBuffer`, or serialize to `String`, before
//! sending to another thread.
//!
//! But when merging rows in different `GridBuffer` into a new `GridBuffer`, we need to `malloc` memory
//! and copying data. This is not efficient. But right now we don't have a better idea to avoid it.
//! There should be a way to avoid it, maybe some kind of customer memory allocator. Leave it as future
//! optimization.
//!
//! Also we need to track the reference count of each `GridBuffer`, so when all rows in the `GridBuffer`
//! are merged into the new `GridBuffer`, we can release the `GridBuffer`.
//!
//! Seems feasible.
//!
//! So we can use an array with a min-heap to sort the data. Full implementation refer to `droplet_core::core::WindowHeap`.
//!
//! We can use this method to achieve multi-level of sorting before the data is stored into file finally.
//!
//! 1. `Sinker` sort the data in each thread locally.
//! 2. Multi `Sinker` on one `Sinker` worker merge and sort data before send to storage `worker` node.
//! 3. Storage `worker` has multi threads too. Each thread merge and sort data from multiple `Sinker` worker.
//! 4. For performance reason, we use multi threads to save the data into file. Each thread is responsible
//! for saving to one file.
//! 5. After all `Sinker` worker and storage worker finish, the data is sorted and stored into one or
//! multiple files. Another process will merge these files into one file or multiple files. If the result
//! has multiple files, the file name will be in order too.
//!
//! Then we have a global ordered dataset, with clear partition.

#![allow(dead_code)]

pub mod feature_sinker;
pub mod grid_sinker;
