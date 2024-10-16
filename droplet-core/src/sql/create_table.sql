-- create database droplet
CREATE DATABASE IF NOT EXISTS droplet;

-- create table for id mapping
CREATE TABLE IF NOT EXISTS id_mapping (
    -- name cannot be longer than 255 characters.
    name VARCHAR(255) NOT NULL,

    -- id is auto increment, use as global id
    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,

    -- created_at is the time when the record is created
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- key for name
    KEY (name)
);

-- create table for feature info
CREATE TABLE IF NOT EXISTS feature_info (
    feature_name VARCHAR(255) NOT NULL PRIMARY KEY,
    data_type INT NOT NULL
);
