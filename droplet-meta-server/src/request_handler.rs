use anyhow::Result;
use log::{error, info};

use std::sync::Arc;

use mysql::*;

use tonic::{Request, Response, Status};

use droplet_core::droplet::meta_server::Meta;
use droplet_core::droplet::{
    GetPartitionInfoRequest, GetPartitionInfoResponse, GetTableInfoRequest, GetTableInfoResponse,
    GetWorkerNodeIdRequest, GetWorkerNodeIdResponse, HeartbeatRequest, HeartbeatResponse,
    InsertTableInfoRequest, InsertTableInfoResponse, RegisterNodeRequest, RegisterNodeResponse,
    ReportStorageInfoRequest, ReportStorageInfoResponse,
};

use droplet_core::db::db::DB;
use droplet_core::db::meta_info::get_partition_infos;
use droplet_core::db::meta_info::insert_table_info;
use droplet_core::db::meta_info::{
    get_partition_count_per_day, get_table_column_infos, get_worker_node_id, register_node,
    update_storage_info,
};
use droplet_core::grpc_util::get_error_status;
use droplet_core::print_and_send_error_status;

pub struct MetaServerImpl {
    /// Db for meta server.
    db: Arc<DB>,
}

impl MetaServerImpl {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    /// Get db connection.
    ///
    /// For more clear log, define as method of MetaServerImpl.
    fn get_db_conn(&self) -> Result<PooledConn, Status> {
        match self.db.get_conn() {
            Ok(conn) => Ok(conn),
            Err(e) => Err(get_error_status(format!(
                "Failed to get db connection for meta server: {}",
                e
            ))),
        }
    }
}

#[tonic::async_trait]
impl Meta for MetaServerImpl {
    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        info!("heartbeat");

        let response = HeartbeatResponse { acknowledged: true };

        Ok(Response::new(response))
    }

    /// Register a new node.
    ///
    /// Register with node name, node ip and node port. And return the node id.
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();

        let mut conn = self.get_db_conn()?;

        let node_id = register_node(
            &mut conn,
            req.node_name.as_str(),
            req.node_ip.as_str(),
            req.node_port,
        )
        .map_err(|e| {
            print_and_send_error_status!("Failed to register node: {}", e);
        })?;

        let response = RegisterNodeResponse {
            node_id,
            success: true,
            error_message: String::new(),
        };

        Ok(Response::new(response))
    }

    async fn get_worker_node_id(
        &self,
        request: Request<GetWorkerNodeIdRequest>,
    ) -> Result<Response<GetWorkerNodeIdResponse>, Status> {
        let req = request.into_inner();

        let mut conn = self.get_db_conn()?;

        let node_id = get_worker_node_id(&mut conn, req.node_name.as_str()).map_err(|e| {
            print_and_send_error_status!("Failed to get worker node id: {}", e);
        })?;

        let response = GetWorkerNodeIdResponse {
            node_id,
            error_message: String::new(),
        };

        Ok(Response::new(response))
    }

    async fn insert_table_info(
        &self,
        request: Request<InsertTableInfoRequest>,
    ) -> Result<Response<InsertTableInfoResponse>, Status> {
        let req = request.into_inner();

        let mut conn = self.get_db_conn()?;

        insert_table_info(
            &mut conn,
            req.table_name.as_str(),
            req.partition_count_per_day,
            &req.columns,
        )
        .map_err(|e| {
            print_and_send_error_status!("Failed to insert table info: {}", e);
        })?;

        let response = InsertTableInfoResponse {
            success: true,
            error_message: String::new(),
        };

        Ok(Response::new(response))
    }

    async fn get_table_info(
        &self,
        request: Request<GetTableInfoRequest>,
    ) -> Result<Response<GetTableInfoResponse>, Status> {
        let req = request.into_inner();

        let mut conn = self.get_db_conn()?;

        let columns = get_table_column_infos(&mut conn, req.table_name.as_str()).map_err(|e| {
            print_and_send_error_status!("Failed to get table columns: {}", e);
        })?;

        let partition_count_per_day =
            get_partition_count_per_day(&mut conn, req.table_name.as_str()).map_err(|e| {
                print_and_send_error_status!("Failed to get partition count per day: {}", e);
            })?;

        let response = GetTableInfoResponse {
            columns,
            partition_count_per_day,
        };

        Ok(Response::new(response))
    }

    async fn report_storage_info(
        &self,
        request: Request<ReportStorageInfoRequest>,
    ) -> Result<Response<ReportStorageInfoResponse>, Status> {
        let req = request.into_inner();

        let mut conn = self.get_db_conn()?;

        update_storage_info(&mut conn, req.node_id, req.used_disk_size).map_err(|e| {
            print_and_send_error_status!("Failed to update storage info: {}", e);
        })?;

        let response = ReportStorageInfoResponse { success: true };

        Ok(Response::new(response))
    }

    async fn get_partition_info(
        &self,
        request: Request<GetPartitionInfoRequest>,
    ) -> Result<Response<GetPartitionInfoResponse>, Status> {
        let req = request.into_inner();

        let mut conn = self.get_db_conn()?;

        let partition_infos =
            get_partition_infos(&mut conn, req.table_name.as_str(), req.timestamp).map_err(
                |e| {
                    print_and_send_error_status!("Failed to get partition info: {}", e);
                },
            )?;

        let response = GetPartitionInfoResponse { partition_infos };

        Ok(Response::new(response))
    }
}
