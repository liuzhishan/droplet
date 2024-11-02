-- create database droplet
CREATE DATABASE IF NOT EXISTS droplet;

-- create table for id mapping
CREATE TABLE IF NOT EXISTS id_mapping (
    key_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY COMMENT 'global unique id',
    key_str VARCHAR(255) NOT NULL COMMENT 'key string, cannot be longer than 255 characters',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'created time',
    KEY (key_str)
);

-- create table for feature info
CREATE TABLE IF NOT EXISTS feature_info (
    feature_name VARCHAR(255) NOT NULL PRIMARY KEY,
    data_type INT NOT NULL
);


CREATE TABLE worker_node_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    node_name VARCHAR(255) NOT NULL COMMENT 'node name',
    node_ip VARCHAR(255) NOT NULL COMMENT 'node ip',
    node_port INT NOT NULL COMMENT 'node port',
    node_status INT NOT NULL COMMENT 'node status, 0 for down, 1 for alive',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'created time',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'updated time',
    UNIQUE KEY (node_name)
);

CREATE TABLE table_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL COMMENT 'table name',
    partition_count_per_day INT NOT NULL COMMENT 'partition count per day',
    UNIQUE KEY (table_name)
);

CREATE TABLE column_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL COMMENT 'table name',
    column_name VARCHAR(255) NOT NULL COMMENT 'column name',
    column_type INT NOT NULL COMMENT 'column type',
    column_index INT NOT NULL COMMENT 'column index',
    column_id INT NOT NULL COMMENT 'global unique id for column according to id_mapping',
    column_comment VARCHAR(255) NOT NULL COMMENT 'column comment',
    UNIQUE KEY (table_name, column_name, column_id)
);

CREATE TABLE partition_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL COMMENT 'table name',
    partition_date INT NOT NULL COMMENT 'partition date, format: YYYYMMDD',
    partition_index INT NOT NULL COMMENT 'partition index',
    node_id INT NOT NULL COMMENT 'node id',
    time_start TIMESTAMP NOT NULL COMMENT 'time start',
    time_end TIMESTAMP NOT NULL COMMENT 'time end',
    UNIQUE KEY (table_name, partition_date, partition_index)
);