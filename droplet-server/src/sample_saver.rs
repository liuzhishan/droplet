use anyhow::{anyhow, bail, Result};
use dashmap::DashMap;
use droplet_core::{droplet::SinkGridSampleRequest, window_heap::WindowHeap};
use likely_stable::unlikely;
use log::{error, info};
use std::fs::File;
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle, Toplevel};

use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;

use std::time::Duration;

use std::sync::Arc;
use sync_unsafe_cell::SyncUnsafeCell;

use gridbuffer::core::gridbuffer::GridBuffer;

use droplet_core::error_bail;

#[derive(Default, Debug, Clone, Eq, PartialEq)]
enum WorkerState {
    #[default]
    Running,
    Failed,
    Success,
}

#[derive(Default)]
pub struct WorkerInfo {
    worker_id: u32,
    total: u64,
    worker_state: WorkerState,
}

unsafe impl Send for WorkerInfo {}
unsafe impl Sync for WorkerInfo {}

/// `SampleSaver` is responsible for saving `GridSample`s to different partitions.
///
/// One `SampleSaver` is responsible for one partition. The data would come from multiple `sinker`s.
/// If all `sinker`s are done with the partition, the `SampleSaver` would be dropped.
///
/// There are multiple concurrent tasks involved. Such as sorting `GridSample`s by `SampleKey`,
/// save to different files, merge files, etc.
///
/// Because there may be hundreds of `sinker`s, if we have one thread for each `sinker`, there
/// would be too much threads. So we dispatch the request to fixed number of threads.
pub struct SampleSaver {
    /// Path.
    path: String,

    /// Path id.
    path_id: u32,

    /// Partition index.
    partition_index: u32,

    /// Current file name.
    cur_filename: String,

    /// All file names under `path`.
    filenames: Vec<String>,

    /// All `sinker` ids.
    ///
    /// Key: `sinker` id.
    /// Value: `true` if the `sinker` is connected. One `sinker` is a thread of one `sinker` worker.
    ///
    /// `grpc` request `start_sink_partition` and `finish_sink_partition` are used to update the `sinker_ids`.
    sinker_ids: DashMap<u32, bool>,

    /// Sender of `SinkGridSampleRequest`.
    sender: async_channel::Sender<SinkGridSampleRequest>,

    /// Number of workers.
    worker_num: u32,

    /// Worker states.
    ///
    /// For performance, we use `SyncUnsafeCell` to access the `WorkerState` directly.
    worker_infos: Vec<Arc<SyncUnsafeCell<WorkerInfo>>>,

    /// Path of final sorted file.
    path_sorted: String,

    /// Batch size for the final merge sort.
    batch_size: u32,

    /// Window size for final merge sort.
    window_size: u32,
}

impl SampleSaver {
    pub fn new(path: &str, path_id: u32, partition_index: u32) -> Result<Self> {
        let (sender, receiver) = async_channel::bounded::<SinkGridSampleRequest>(256);

        let worker_num = 8;

        let worker_infos = vec![Arc::new(SyncUnsafeCell::new(WorkerInfo::default())); worker_num];

        let mut filenames = Vec::with_capacity(worker_num);
        for i in 0..worker_num {
            filenames.push(format!("{}/{}.grid", path, i));
        }

        {
            let filenames_clone = filenames.clone();
            Self::start_worker(receiver, &filenames_clone, path, path_id, &worker_infos);
        }

        let path_sorted = path.replace("droplet", "droplet_sorted").to_string();

        std::fs::create_dir_all(path)?;
        std::fs::create_dir_all(path_sorted.clone())?;

        Ok(Self {
            path: path.to_string(),
            path_id,
            partition_index,
            cur_filename: "".to_string(),
            filenames,
            sinker_ids: DashMap::new(),
            sender,
            worker_num: worker_num as u32,
            worker_infos,
            path_sorted,
            batch_size: 4,
            window_size: 256,
        })
    }

    fn start_worker(
        receiver: async_channel::Receiver<SinkGridSampleRequest>,
        filenames: &Vec<String>,
        _path: &str,
        path_id: u32,
        worker_infos: &Vec<Arc<SyncUnsafeCell<WorkerInfo>>>,
    ) {
        for (i, filename) in filenames.iter().enumerate() {
            info!("start sample saver worker {}", i);
            let index = i as u32;
            let new_receiver = receiver.clone();
            let path_id_clone = path_id.clone();
            let filename_clone = filename.clone();
            let worker_info_clone = worker_infos[i].clone();

            let worker_name = format!("sample_saver_worker_{}_{}", path_id_clone, index);

            let worker = SampleSaverWorker::new(
                index,
                filename_clone.as_str(),
                new_receiver,
                worker_info_clone,
            );

            tokio::spawn(async move {
                let _ = Toplevel::new(|s| async move {
                    s.start(SubsystemBuilder::new(worker_name, |s| async move {
                        worker.run(s).await
                    }));
                })
                .catch_signals()
                .handle_shutdown_requests(Duration::from_millis(1000))
                .await;
            });
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn start_partition(&self, sinker_id: u32) {
        self.sinker_ids.insert(sinker_id, true);
    }

    pub fn finish_partition(&self, sinker_id: u32) {
        self.sinker_ids.remove(&sinker_id);
    }

    pub fn close_sender(&self) {
        self.sender.close();
    }

    pub fn is_sinkers_done(&self) -> bool {
        self.sinker_ids.len() == 0
    }

    pub fn is_workers_done(&self) -> bool {
        self.worker_infos.iter().all(|x| {
            let worker_info = unsafe { &*x.get() };
            worker_info.worker_state == WorkerState::Success
        })
    }

    pub async fn process(&self, req: SinkGridSampleRequest) -> Result<()> {
        self.sender
            .send(req)
            .await
            .map_err(|_| anyhow!("send request to sample saver failed"))
    }

    pub fn is_success(&self) -> bool {
        self.worker_infos.iter().all(|x| {
            let worker_info = unsafe { &*x.get() };
            worker_info.worker_state == WorkerState::Success
        })
    }

    pub fn is_failed(&self) -> bool {
        self.worker_infos.iter().any(|x| {
            let worker_info = unsafe { &*x.get() };
            worker_info.worker_state == WorkerState::Failed
        })
    }

    /// Use empty file `SUCCESS` to indicate the partition is done.
    pub fn write_success_file(&self) -> Result<()> {
        if unlikely(self.path.is_empty()) {
            error_bail!("path is empty");
        }

        let filename = format!("{}/SUCCESS", self.path.to_string());

        File::create(filename)?;

        Ok(())
    }

    fn get_total_lines(&self) -> u64 {
        self.worker_infos
            .iter()
            .map(|x| unsafe {
                let worker_info = &*x.get();
                worker_info.total
            })
            .sum()
    }

    pub fn merge_sort(&self) -> Result<()> {
        if !self.is_workers_done() {
            error_bail!(
                "sample saver workers are not done, path: {}",
                self.path.clone()
            );
        }

        let mut readers = Vec::with_capacity(self.worker_num as usize);
        for (_i, filename) in self.filenames.iter().enumerate() {
            let file = File::open(filename)?;
            readers.push(BufReader::new(file));
        }

        let mut window_heap = WindowHeap::new(self.window_size as usize, self.batch_size as usize);

        let mut count_done = 0;
        let mut last_reader_index = 0;

        let mut is_full = false;
        // Read lines until window heap is full.
        for _i in 0..self.window_size {
            if is_full {
                break;
            }

            for j in 0..readers.len() {
                let mut line = String::new();
                match readers[j].read_line(&mut line) {
                    Ok(0) => {
                        count_done += 1;

                        if count_done == readers.len() {
                            break;
                        }
                    }
                    Ok(_) => {
                        let line = line.trim_end();
                        let gridbuffer = GridBuffer::from_base64(line)?;

                        last_reader_index = j;

                        match window_heap.push_with_reader_index(gridbuffer, j) {
                            Ok(_) => {
                                if window_heap.is_full() {
                                    is_full = true;
                                    break;
                                }
                            }
                            Err(err) => {
                                error!("push gridbuffer to window heap failed, error: {}", err);
                            }
                        }
                    }
                    Err(_err) => {
                        count_done += 1;

                        if count_done == readers.len() {
                            break;
                        }
                    }
                }
            }
        }

        let mut cur_file_index = 0;
        let mut file =
            File::create(format!("{}/{}.grid", self.path_sorted, cur_file_index).as_str())?;
        let mut count_write_line = 0;

        let total_lines = self.get_total_lines();
        let lines_per_file = total_lines / self.worker_num as u64;

        while count_done < readers.len() {
            let mut line = String::new();
            match readers[last_reader_index].read_line(&mut line) {
                Ok(0) => {
                    count_done += 1;
                    last_reader_index = (last_reader_index + 1) % readers.len();
                }
                Ok(_) => {
                    let line = line.trim_end();
                    let gridbuffer = GridBuffer::from_base64(line)?;

                    window_heap.push(gridbuffer)?;

                    self.process_out_gridbuffers(
                        &mut window_heap,
                        &mut file,
                        &mut count_write_line,
                        &mut cur_file_index,
                        lines_per_file,
                        &mut last_reader_index,
                    )?;
                }
                Err(err) => {
                    count_done += 1;
                    info!("read line from reader done, error: {}", err);
                }
            }
        }

        window_heap.process_remain_data();

        self.process_out_gridbuffers(
            &mut window_heap,
            &mut file,
            &mut count_write_line,
            &mut cur_file_index,
            lines_per_file,
            &mut last_reader_index,
        )?;

        Ok(())
    }

    fn process_out_gridbuffers(
        &self,
        window_heap: &mut WindowHeap,
        file: &mut File,
        count_write_line: &mut u64,
        cur_file_index: &mut u32,
        lines_per_file: u64,
        last_reader_index: &mut usize,
    ) -> Result<()> {
        if window_heap.out_gridbuffers().len() > 0 {
            while let Some(gridbuffer) = window_heap.get_out_gridbuffer() {
                file.write_all(gridbuffer.to_base64().as_bytes())?;
                file.write_all(b"\n")?;

                *count_write_line += 1;
                if *count_write_line >= lines_per_file {
                    *cur_file_index += 1;
                    *file = File::create(
                        format!("{}/{}.grid", self.path_sorted, cur_file_index).as_str(),
                    )?;
                    *count_write_line = 0;
                }
            }

            if let Some(reader_index) = window_heap.get_out_reader_index() {
                *last_reader_index = reader_index;
            }
        }

        Ok(())
    }
}

pub struct SampleSaverWorker {
    /// Path.
    filename: String,

    /// Worker id.
    worker_id: u32,

    /// Receiver of `SinkGridSampleRequest`.
    receiver: async_channel::Receiver<SinkGridSampleRequest>,

    /// Window heap for sorting `GridSample`s.
    window_heap: WindowHeap,

    /// Window size.
    window_size: u32,

    /// Batch size.
    batch_size: u32,

    /// Worker states.
    worker_info: Arc<SyncUnsafeCell<WorkerInfo>>,
}

impl SampleSaverWorker {
    pub fn new(
        worker_id: u32,
        filename: &str,
        receiver: async_channel::Receiver<SinkGridSampleRequest>,
        worker_info: Arc<SyncUnsafeCell<WorkerInfo>>,
    ) -> Self {
        let window_size = 256;
        let batch_size = 4;

        Self {
            filename: filename.to_string(),
            worker_id,
            receiver,
            window_heap: WindowHeap::new(window_size, batch_size),
            window_size: window_size as u32,
            batch_size: batch_size as u32,
            worker_info,
        }
    }

    fn set_worker_state(&self, state: WorkerState) {
        let worker_info = unsafe { &mut *self.worker_info.get() };
        worker_info.worker_state = state;
    }

    pub async fn run(mut self, subsys: SubsystemHandle) -> Result<()> {
        if unlikely(self.filename.is_empty()) {
            error_bail!("filename is empty, worker_id: {}", self.worker_id);
        }

        let mut file = File::create(self.filename.as_str())?;

        loop {
            tokio::select! {
                req = self.receiver.recv() => {
                    match req {
                        Ok(req) => {
                            let gridbuffer = GridBuffer::from_bytes(&req.grid_sample_bytes)?;

                            self.window_heap.push(gridbuffer)?;

                            if self.window_heap.out_gridbuffers().len() > 0 {
                                while let Some(gridbuffer) = self.window_heap.get_out_gridbuffer() {
                                    info!(
                                        "Get out gridbuffer, size: {}, worker_id: {}",
                                        gridbuffer.estimated_bytes(),
                                        self.worker_id
                                    );

                                    file.write_all(gridbuffer.to_base64().as_bytes())?;
                                    file.write_all(b"\n")?;

                                    let worker_info = unsafe { &mut *self.worker_info.get() };
                                    worker_info.total += 1;
                                }
                            }
                        }
                        Err(err) => {
                            info!("receive request error! read data done, error: {}", err);
                            self.set_worker_state(WorkerState::Success);
                            break;
                        }
                    }
                },
                _ = subsys.on_shutdown_requested() => {
                    info!("sample saver worker shutdown!");
                    self.set_worker_state(WorkerState::Success);
                    break;
                }
            }
        }

        self.window_heap.process_remain_data();

        if self.window_heap.out_gridbuffers().len() > 0 {
            while let Some(gridbuffer) = self.window_heap.get_out_gridbuffer() {
                file.write_all(gridbuffer.to_base64().as_bytes())?;
                file.write_all(b"\n")?;

                let worker_info = unsafe { &mut *self.worker_info.get() };
                worker_info.total += 1;
            }
        }

        info!(
            "sample saver worker done, filename: {}",
            self.filename.clone()
        );

        Ok(())
    }
}
