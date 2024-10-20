# Meta Server

保存各种元数据信息，如 `worker` 节点、表、分区、路径、列名、列类型、列值等信息。采用 `mysql` 作为存储。

## 元信息

### `id_mapping`

保存全局唯一 `String` 到 `id` 映射关系。

表结构如下

    CREATE TABLE id_mapping (
        key_id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY COMMENT 'global unique id',
        key_str VARCHAR(255) NOT NULL COMMENT 'key string, cannot be longer than 255 characters',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'created time',
        UNIQUE KEY (key_str)
    );


### `table_info`

表的元数据信息，如表的名称等信息。列名单独用另一张表来保存。

表结构如下

    CREATE TABLE table_info (
        id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        table_name VARCHAR(255) NOT NULL COMMENT 'table name',
        partition_count_per_day INT UNSIGNED NOT NULL COMMENT 'partition count per day',
        UNIQUE KEY (table_name)
    );

### `column_info`

列的元数据信息，如列名称、列类型、列值等信息。

表结构如下

    CREATE TABLE column_info (
        id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        table_name VARCHAR(255) NOT NULL COMMENT 'table name',
        column_name VARCHAR(255) NOT NULL COMMENT 'column name',
        column_type VARCHAR(255) NOT NULL COMMENT 'column type',
        column_index INT UNSIGNED NOT NULL COMMENT 'column index',
        column_id INT UNSIGNED NOT NULL COMMENT 'global unique id for column according to id_mapping',
        column_comment VARCHAR(255) NOT NULL COMMENT 'column comment',
        UNIQUE KEY (table_name, column_name, column_id)
    );


### `partition_info`

分区的元数据信息，如分区名称、分区路径、分区大小、分区时间戳等信息。

存储根目录固定为 `~/documents/droplet_table`。

表结构如下

    CREATE TABLE partition_info (
        id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        table_name VARCHAR(255) NOT NULL COMMENT 'table name',
        partition_date INT UNSIGNED NOT NULL COMMENT 'partition date, format: YYYYMMDD',
        partition_index INT UNSIGNED NOT NULL COMMENT 'partition index',
        node_id INT UNSIGNED NOT NULL COMMENT 'node id',
        UNIQUE KEY (table_name, partition_date, partition_index)
    );


### `worker_node_info`

`worker` 节点的元数据信息，如 `worker` 节点的名称、IP 地址、端口、状态、分区信息等。

表结构如下

    CREATE TABLE worker_node_info (
        id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        node_name VARCHAR(255) NOT NULL COMMENT 'node name',
        node_ip VARCHAR(255) NOT NULL COMMENT 'node ip',
        node_port INT UNSIGNED NOT NULL COMMENT 'node port',
        node_status INT NOT NULL COMMENT 'node status, 0 for down, 1 for alive',
        total_disk_size BIGINT UNSIGNED NOT NULL COMMENT 'total disk size',
        UNIQUE KEY (node_name)
    );

### `node_storage_info`

保存 `worker` 节点定期更新的存储信息。

表结构如下

    CREATE TABLE node_storage_info (
        node_id INT UNSIGNED NOT NULL COMMENT 'node id',
        used_disk_size BIGINT UNSIGNED NOT NULL COMMENT 'used disk size',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'updated time',
        PRIMARY KEY (node_id, updated_at)
    );


## 接口

### `heartbeat`

`client` 向 `meta` 节点发送心跳，心跳信息包括 `client` 的名称等。

接口格式如下

    message HeartbeatRequest {
        uint32 node_id = 1;
        NodeStatus status = 2;
    }

    message HeartbeatResponse {
        bool acknowledged = 1;
    }

### `register_node`

注册 `worker` 节点。

接口格式如下

    message RegisterNodeRequest {
        string node_name = 1;
        string node_ip = 2;
        uint32 node_port = 3;
    }

    message RegisterNodeResponse {
        uint32 node_id = 1;
        bool success = 2;
        string error_message = 3;
    }


### `get_worker_node_id`

根据 `node_name` 获取 `worker` 节点的 `node_id`。

接口格式如下

    message GetWorkerNodeIdRequest {
        string node_name = 1;
    }

    message GetWorkerNodeIdResponse {
        uint32 node_id = 1;
        string error_message = 2;
    }


### `get_worker_node_info`

根据 `node_id` 获取 `worker` 节点的元数据信息。

接口格式如下

    message GetWorkerNodeInfoRequest {
        uint32 node_id = 1;
    }

    message GetWorkerNodeInfoResponse {
        string node_name = 1;
        string node_ip = 2;
        uint32 node_port = 3;
    }

### `insert_table_info`

插入表的元数据信息。

接口格式如下

    message InsertTableInfoRequest {
        string table_name = 1;
        uint32 partition_count_per_day = 2;
        repeated ColumnInfo columns = 3;
    }

    message InsertTableInfoResponse {
        bool success = 1;
        string error_message = 2;
    }

### `get_table_info`

获取表的元数据信息。如表的列名、列类型等。

接口格式如下

    message GetTableInfoRequest {
        string table_name = 1;
    }

    message ColumnInfo {
        string column_name = 1;
        string column_type = 2;
        uint32 column_id = 3;
        uint32 column_index = 4;
    }

    message GetTableInfoResponse {
        repeated ColumnInfo columns = 1;
        uint32 partition_count_per_day = 2;
    }


### `report_storage_info`

为了及时计算 `partition` 信息，`worker` 存储节点需要及时上报本机剩余存储空间。考虑到可能有上万台 `worker` 节点，按 `5` 分钟进行上报。
在计算 `partition` 信息是，根据 `worker` 剩余空间来进行计算，首先选择剩余空间最大的 `worker` 节点，并在 `meta server` `table`
中记录 `partition` 结果。

第一期可以直接用 `sql` 进行计算。之后可以专门在内存中维护相关信息，进行实时计算。

接口格式如下

    message ReportStorageInfoRequest {
        uint32 node_id = 1;
        uint64 used_disk_size = 2;
    }

    message ReportStorageInfoResponse {
        bool success= 1;
    }


### `get_partition_infos`

根据 `table` 名获取分区的元数据信息。

按照时间进行分区，所以只需要获取存储节点机器名以及路径即可。具体的分区可以通过代码进行约束。我们可以定义
一个分区的规则。

由于所有的数据都按时间来进行分区，因此我们尽量以时间来规定分区。

如下是第一版的规则。
1. 由于数据量不同，为了尽量减少文件个数，并且相对灵活，分区可大可小，但是至少分区到天。
2. 用一个整数来代表一天内的分区个数，通过小时与分钟的换算可以精确表示分区规则。比如 `24` 则表示每个小时
   一个分区，`120` 则表示每个小时内有分了 `120 / 24 = 5` 个分区，即每 `12` 分钟一个分区。
3. 根据样本中的时间戳 `timestamp` 以及 `partition` 个数，则可以确定样本应该保存到哪个分区。 因此，
   `meta server` 中仅需要保存 `table` 名和一天的 `partition` 个数即可。
4. 每个分区内的文件数无法提前确定，因此我们只规定文件名规则，按照 `SampleKey` 的顺序进行排序，文件名以
   数字自增，如 `part-0000.grid` 表示第一个文件，`part-0001.grid` 表示第二个文件，依次类推。并且有
   一个 `SUCCESS` 的空文件表示改分区以处理完毕，可以使用。
5. 每个分区保存到哪个 `worker` 节点，则需要 `meta server` 根据 `worker` 节点的信息来确定。第一版可
   以简单处理，但是考虑到一个分区的所有数据都会发送到同一个 `worker` 节点，因此实际必须处理好负载均衡的
   问题，否则单个 `worker` 节点的负载会很高。这一步之后需要探索不同的策略。

结合以上分析，此接口请求和响应格式如下：

    message GetPartitionInfoRequest {
        string table_name = 1;
        uint64 timestamp = 2;
    }

    message PartitionInfo {
        uint32 partition_id = 1;
        uint32 partition_date = 2;
        uint32 partition_index = 3;
        uint32 node_id = 4;
        string worker_name = 5;
        string worker_ip = 6;
        uint32 worker_port = 7;
        uint64 time_start = 8;
        uint64 time_end = 9;
    }

    message GetPartitionInfoResponse {
        repeated PartitionInfo partition_infos = 1;
    }
