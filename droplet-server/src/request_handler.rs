use std::time::Duration;

use dashmap::DashMap;

use anyhow::Result;
use log::error;

use std::sync::Arc;

use tonic::{Request, Response, Status};

use droplet_core::droplet::droplet_server::Droplet;
use droplet_core::droplet::{
    FinishSinkPartitionRequest, FinishSinkPartitionResponse, HeartbeatRequest,
    HeartbeatResponse, SinkGridSampleRequest, SinkGridSampleResponse, StartSinkPartitionRequest,
    StartSinkPartitionResponse,
};

use droplet_core::db::db::DB;
use droplet_core::db::meta_info::get_or_insert_key_id;
use droplet_core::grpc_util::send_error_message;

use crate::sample_saver::SampleSaver;

/// Droplet server implementation.
///
/// To speedup storing `GridSample`s, we need to use multiple threads. Each thread will write to a different file.
///
/// We also need to handle data from different `sinker`s, which means we need to dispatch each request to different
/// worker threads that are dedicated to a particular `sinker`.
pub struct DropletServerImpl {
    db: Arc<DB>,

    /// Different sample savers.
    ///
    /// For performance consideration, we use global key of `path` as the key of `DashMap`, instead of `String`.
    sample_savers: DashMap<u32, SampleSaver>,
}

impl DropletServerImpl {
    pub fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            sample_savers: DashMap::new(),
        }
    }

    fn get_path_id(&self, path: &str) -> u32 {
        let mut conn = self.db.get_conn().unwrap();
        get_or_insert_key_id(&mut conn, path)
    }
}

#[tonic::async_trait]
impl Droplet for DropletServerImpl {
    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse { acknowledged: true }))
    }

    async fn start_sink_partition(
        &self,
        request: Request<StartSinkPartitionRequest>,
    ) -> Result<Response<StartSinkPartitionResponse>, Status> {
        let req = request.into_inner();

        match self.sample_savers.get(&req.path_id) {
            Some(saver) => saver.start_partition(req.sinker_id),
            None => {
                let saver =
                    match SampleSaver::new(req.path.as_str(), req.path_id, req.partition_index) {
                        Ok(saver) => saver,
                        Err(e) => {
                            error!(
                                "Create sample saver failed, path: {}, error: {}",
                                req.path.clone(),
                                e
                            );
                            return send_error_message::<StartSinkPartitionResponse>(format!(
                                "Create sample saver failed, path: {}, error: {}",
                                req.path.clone(),
                                e
                            ));
                        }
                    };

                saver.start_partition(req.sinker_id);

                self.sample_savers.insert(req.path_id, saver);
            }
        }

        Ok(Response::new(StartSinkPartitionResponse { success: true }))
    }

    async fn sink_grid_sample(
        &self,
        request: Request<SinkGridSampleRequest>,
    ) -> Result<Response<SinkGridSampleResponse>, Status> {
        let req = request.into_inner();

        let path_id = req.path_id;

        match self.sample_savers.get(&path_id) {
            Some(saver) => match saver.process(req).await {
                Ok(_) => {}
                Err(e) => {
                    error!("Save has error, path_id: {}, error: {}", path_id, e);
                    return send_error_message::<SinkGridSampleResponse>(format!(
                        "Save has error, path_id: {}, error: {}",
                        path_id, e
                    ));
                }
            },
            None => {
                error!("Sample saver not found for path_id: {}", path_id);
                return send_error_message::<SinkGridSampleResponse>(format!(
                    "Sample saver not found for path_id: {}",
                    path_id
                ));
            }
        }

        Ok(Response::new(SinkGridSampleResponse {
            success: true,
            path_id,
            error_message: "".to_string(),
        }))
    }

    async fn finish_sink_partition(
        &self,
        request: Request<FinishSinkPartitionRequest>,
    ) -> Result<Response<FinishSinkPartitionResponse>, Status> {
        let req = request.into_inner();

        let mut is_done = false;

        match self.sample_savers.get(&req.path_id) {
            Some(saver) => {
                saver.finish_partition(req.sinker_id);

                if saver.is_sinkers_done() {
                    saver.close_sender();
                    // Wait the workers done.
                    while !saver.is_workers_done() {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    }

                    match saver.merge_sort() {
                        Ok(_) => {}
                        Err(e) => {
                            error!("Merge files failed, path: {}, error: {}", saver.path(), e);
                            return send_error_message::<FinishSinkPartitionResponse>(format!(
                                "Merge files failed, path: {}, error: {}",
                                saver.path(),
                                e
                            ));
                        }
                    }

                    is_done = true;
                }
            }
            None => {
                error!("Sample saver not found for path_id: {}", req.path_id);
                return send_error_message::<FinishSinkPartitionResponse>(format!(
                    "Sample saver not found for path_id: {}",
                    req.path_id
                ));
            }
        }

        if is_done {
            self.sample_savers.remove(&req.path_id);
        }

        Ok(Response::new(FinishSinkPartitionResponse { success: true }))
    }
}
