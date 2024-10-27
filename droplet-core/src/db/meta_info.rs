use chrono::Duration;
use chrono::Timelike;
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use log::{error, info};
use mysql::params;
use mysql::prelude::*;
use mysql::PooledConn;

use anyhow::{bail, Result};

use crate::droplet::ColumnInfo;
use crate::droplet::NodeInfo;
use crate::droplet::NodeStatus;
use crate::droplet::PartitionInfo;
use crate::error_bail;

/// Get key id from `id_mapping` table.
pub fn get_key_id(conn: &mut PooledConn, key_str: &str) -> Option<u32> {
    match conn.query_first::<u32, _>(format!(
        "SELECT key_id FROM id_mapping WHERE key_str = '{}'",
        key_str.to_string()
    )) {
        Ok(key_id) => key_id,
        Err(e) => {
            error!(
                "Failed to get key id from id_mapping, key_str: {}, error: {:?}",
                key_str, e
            );
            None
        }
    }
}

pub fn get_key_ids(conn: &mut PooledConn, keys: &Vec<String>) -> Result<Vec<u32>> {
    Ok(keys.iter().map(|k| get_or_insert_key_id(conn, k)).collect())
}

/// Insert key string into `id_mapping` table.
pub fn insert_key_str(conn: &mut PooledConn, key_str: &str) -> Result<()> {
    conn.exec_drop(
        "INSERT INTO id_mapping (key_str) VALUES (:key_str)",
        params! { "key_str" => key_str.to_string() },
    )
    .map_err(|e| {
        anyhow::anyhow!(
            "Failed to insert key string into id_mapping, key_str: {}, error: {:?}",
            key_str,
            e
        )
    })
}

/// Get key id from `id_mapping` table.
///
/// If the key string does not exist, insert it into the table and return the new key id.
pub fn get_or_insert_key_id(conn: &mut PooledConn, key_str: &str) -> u32 {
    match get_key_id(conn, key_str) {
        Some(key_id) => key_id,
        None => {
            match insert_key_str(conn, key_str) {
                Ok(_) => match get_key_id(conn, key_str) {
                    Some(key_id) => key_id,
                    None => {
                        // Unlikely to happen.
                        error!("Failed to get key id after inserting, key_str: {}", key_str);
                        0
                    }
                },
                Err(e) => {
                    // Unlikely to happen.
                    error!(
                        "Failed to insert key string into id_mapping, key_str: {}, error: {:?}",
                        key_str, e
                    );
                    0
                }
            }
        }
    }
}

/// Register node info.
///
/// If the node name already exists, return the existing node id.
/// Else insert a new record into the table `worker_node_info` and return the new node id.
pub fn register_node(
    conn: &mut PooledConn,
    node_name: &str,
    node_ip: &str,
    node_port: u32,
) -> Result<u32> {
    let node_id = conn.query_first::<u32, _>(format!(
        "SELECT id FROM worker_node_info WHERE node_name = '{}'",
        node_name
    ))?;

    if let Some(node_id) = node_id {
        return Ok(node_id);
    }

    conn.exec_drop(
        "INSERT INTO worker_node_info (node_name, node_ip, node_port, node_status) VALUES (:node_name, :node_ip, :node_port, 1)",
        params! {
            "node_name" => node_name,
            "node_ip" => node_ip,
            "node_port" => node_port,
        }
    )?;

    match conn.query_first::<u32, _>(format!(
        "SELECT id FROM worker_node_info WHERE node_name = '{}'",
        node_name
    ))? {
        Some(node_id) => Ok(node_id),
        None => bail!("Failed to get node id"),
    }
}

pub fn get_worker_node_id(conn: &mut PooledConn, node_name: &str) -> Result<u32> {
    match conn.query_first::<u32, _>(format!(
        "SELECT id FROM worker_node_info WHERE node_name = '{}'",
        node_name.to_string()
    ))? {
        Some(node_id) => Ok(node_id),
        None => bail!("Node not found, node_name: {}", node_name.to_string()),
    }
}

pub fn insert_table_info(
    conn: &mut PooledConn,
    table_name: &str,
    partition_count_per_day: u32,
    columns: Vec<ColumnInfo>,
) -> Result<()> {
    // Insert table info.
    conn.exec_drop(
        "INSERT INTO table_info (table_name, partition_count_per_day) VALUES (:table_name, :partition_count_per_day)",
        params! {
            "table_name" => table_name.to_string(),
            "partition_count_per_day" => partition_count_per_day,
        }
    )?;

    // Insert column name into id_mapping.
    let stmt_id_mapping = "INSERT IGNORE INTO id_mapping (key_str) VALUES (:column_name)";
    conn.exec_batch(
        stmt_id_mapping,
        columns.iter().map(|c| {
            params! {
                "column_name" => c.column_name.to_string(),
            }
        }),
    )?;

    // Insert column infos.
    let stmt_column_infos = "INSERT IGNORE INTO
        column_info (table_name, column_name, column_type, column_id, column_index)
    SELECT :table_name, :column_name, :column_type, id_mapping.key_id, :column_index
    FROM id_mapping
    WHERE id_mapping.key_str = :column_name";
    conn.exec_batch(
        stmt_column_infos,
        columns.iter().map(|c| {
            params! {
                "table_name" => table_name.to_string(),
                "column_name" => c.column_name.to_string(),
                "column_type" => c.column_type.to_string(),
                "column_index" => c.column_index,
            }
        }),
    )?;

    Ok(())
}

pub fn get_table_column_infos(conn: &mut PooledConn, table_name: &str) -> Result<Vec<ColumnInfo>> {
    conn.query_map(
        format!(
            "SELECT 
            column_name, 
            column_type, 
            column_id, 
            column_index 
        FROM table_columns 
        WHERE table_name = '{}'",
            table_name.to_string()
        ),
        |row: (String, String, u32, u32)| ColumnInfo {
            column_name: row.0,
            column_type: row.1,
            column_id: row.2,
            column_index: row.3,
        },
    )
    .map_err(|e| {
        anyhow::anyhow!(
            "Failed to get table column infos, table_name: {}, error: {:?}",
            table_name,
            e
        )
    })
}

pub fn get_partition_count_per_day(conn: &mut PooledConn, table_name: &str) -> Result<u32> {
    match conn.query_first::<u32, _>(format!(
        "SELECT partition_count_per_day FROM table_info WHERE table_name = '{}'",
        table_name.to_string()
    ))? {
        Some(partition_count_per_day) => Ok(partition_count_per_day),
        None => bail!(
            "Table not found for partition count per day, table_name: {}",
            table_name.to_string()
        ),
    }
}

pub fn update_storage_info(conn: &mut PooledConn, node_id: u32, used_disk_size: u64) -> Result<()> {
    conn.exec_drop(
        "INSERT INTO node_storage_info (node_id, used_disk_size) VALUES (:node_id, :used_disk_size)",
        params! {
            "node_id" => node_id,
            "used_disk_size" => used_disk_size,
        }
    )?;
    Ok(())
}

/// Get partition infos by timestamp.
///
/// Return one PartitionInfo now. Maybe more in the future for better performance.
pub fn get_partition_infos(
    conn: &mut PooledConn,
    table_name: &str,
    timestamp: u64,
) -> Result<Vec<PartitionInfo>> {
    let partition_count_per_day = get_partition_count_per_day(conn, table_name)?;

    let naive_datetime = NaiveDateTime::from_timestamp_opt(timestamp as i64, 0)
        .ok_or_else(|| anyhow::anyhow!(format!("Invalid timestamp: {}", timestamp)))?;
    let seconds_in_day = naive_datetime.num_seconds_from_midnight();
    let partition_index = (seconds_in_day as u32 * partition_count_per_day / 86400) as u32;

    let partition_date = naive_datetime.format("%Y%m%d").to_string().parse::<u32>()?;

    let time_span_in_seconds: i64 = 86400 / partition_count_per_day as i64;

    let midnight = naive_datetime - Duration::seconds(seconds_in_day.into());
    let time_start = midnight + Duration::seconds(time_span_in_seconds * partition_index as i64);
    let time_end = time_start + Duration::seconds(time_span_in_seconds);

    let ts = naive_datetime - Duration::minutes(60);
    let available_node = get_available_node(conn, ts)?;

    // Insert partition info into database.
    let partition_id = insert_partition_info(
        conn,
        table_name,
        partition_date,
        partition_index,
        available_node.node_id,
        &time_start,
        &time_end,
    )?;

    let partition_info = PartitionInfo {
        partition_id,
        partition_date,
        partition_index,
        node_id: available_node.node_id,
        node_name: available_node.node_name.to_string(),
        node_ip: available_node.node_ip.to_string(),
        node_port: available_node.node_port,
        time_start: time_start.timestamp_millis() as u64,
        time_end: time_end.timestamp_millis() as u64,
    };

    Ok(vec![partition_info])
}

/// Select the available node with the least disk usage.
///
/// We use sql to select the node, order by `update_at` desc and `disk_usage_ratio` asc.
/// Accoding this rule we can select the node with the least disk usage.
pub fn get_available_node(conn: &mut PooledConn, midnight: NaiveDateTime) -> Result<NodeInfo> {
    let node_usage = conn.query_first::<(u32, String, String, u32, f64), _>(format!(
        "SELECT
            node_id,
            node_name,
            node_ip,
            node_port,
            disk_usage_ratio
        FROM (
            SELECT
                node_id,
                node_name,
                node_ip,
                node_port,
                disk_usage_ratio
            FROM (
                SELECT
                    a.node_id, 
                    a.used_disk_size,
                    a.used_disk_size / b.total_disk_size disk_usage_ratio,
                    a.update_at,
                    b.node_name,
                    b.node_ip,
                    b.node_port,
                    row_number() over (order by a.node_id, a.update_at desc) rank
                FROM node_storage_info a
                JOIN worker_node_info b ON a.node_id = b.id
                AND b.status = 1
                AND a.update_at > '{}'
                AND b.total_disk_size > 0
                ORDER BY a.node_id, a.update_at DESC
            ) t
            WHERE t.rank = 1
        ) t1
        ORDER BY t1.disk_usage_ratio ASC
        LIMIT 1
        ",
        midnight.format("%Y-%m-%d").to_string()
    ))?;

    match node_usage {
        Some(node_usage) => Ok(NodeInfo {
            node_id: node_usage.0,
            node_name: node_usage.1,
            node_ip: node_usage.2,
            node_port: node_usage.3,
            status: NodeStatus::Alive.into(),
        }),
        None => {
            error_bail!("No available node");
        }
    }
}

pub fn insert_partition_info(
    conn: &mut PooledConn,
    table_name: &str,
    partition_date: u32,
    partition_index: u32,
    node_id: u32,
    time_start: &NaiveDateTime,
    time_end: &NaiveDateTime,
) -> Result<u32> {
    conn.exec_drop(
        "INSERT INTO
            partition_info (table_name, partition_date, partition_index, node_id, time_start, time_end)
        VALUES (:table_name, :partition_date, :partition_index, :node_id, :time_start, :time_end)",
        params! {
            "table_name" => table_name.to_string(),
            "partition_date" => partition_date,
            "partition_index" => partition_index,
            "node_id" => node_id,
            "time_start" => time_start.format("%Y-%m-%d %H:%M:%S").to_string(),
            "time_end" => time_end.format("%Y-%m-%d %H:%M:%S").to_string(),
        },
    )?;

    match conn.query_first::<u32, _>(format!(
        "SELECT
            id
        FROM partition_info
        WHERE table_name = '{}' AND partition_date = {} AND partition_index = {} AND node_id = {}",
        table_name.to_string(),
        partition_date,
        partition_index,
        node_id
    ))? {
        Some(partition_id) => Ok(partition_id),
        None => {
            error_bail!("Failed to get partition id");
        }
    }
}

/// Get partition paths for a table.
pub fn get_table_paths_by_time(
    conn: &mut PooledConn,
    table: &str,
    time_start: &NaiveDateTime,
    time_end: &NaiveDateTime,
) -> Result<Vec<String>> {
    let partition_count_per_day = get_partition_count_per_day(conn, table)?;

    let partition_indexes = conn.query_map(
        format!(
            "SELECT
                t.partition_index,
                p.partition_date
            FROM table_info t
            JOIN partition_info p ON t.table_name = p.table_name
            WHERE t.table_name = '{}'
                AND p.time_start <= '{}'
                AND p.time_end >= '{}'
            ",
            table.to_string(),
            time_start.format("%Y-%m-%d %H:%M:%S").to_string(),
            time_end.format("%Y-%m-%d %H:%M:%S").to_string(),
        ),
        |row: (u32, u32)| (row.0, row.1),
    )?;

    let partition_paths = partition_indexes
        .iter()
        .map(|(index, partition_date)| {
            format!("/tmp/droplet/tables/{}/{}/{}", table, partition_date, index)
        })
        .collect();

    Ok(partition_paths)
}

/// Parse a date from a u32 integer in the format YYYYMMDD
fn parse_date_from_u32(p_date: u32) -> Result<NaiveDate> {
    let year = (p_date / 10000) as i32;
    let month = ((p_date % 10000) / 100) as u32;
    let day = (p_date % 100) as u32;

    NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| anyhow::anyhow!("Invalid date: {}", p_date))
}

pub fn get_table_paths_by_date(
    conn: &mut PooledConn,
    table: &str,
    partition_date: u32,
) -> Result<Vec<String>> {
    let partition_count_per_day = get_partition_count_per_day(conn, table)?;
    let partition_indexes = (0..partition_count_per_day).collect::<Vec<u32>>();

    let partition_paths = partition_indexes
        .iter()
        .map(|index| format!("/tmp/droplet/tables/{}/{}/{}", table, partition_date, index))
        .collect();

    Ok(partition_paths)
}
