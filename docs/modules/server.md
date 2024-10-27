# server

`server` 节点是 `droplet` 集群中负责接收和保存数据的节点。

## 注册节点

`server` 启动时候需要向 `meta_server` 注册自己，注册成功后，`server` 会定期向 `meta_server` 发送心跳包，
如果 `meta_server` 长时间收不到 `server` 的心跳，则认为 `server` 已经挂掉。

## 接收与保存数据

`server` 节点的主要功能是接收从 `sinker` 发送过来的 `GridSample` 数据，并保存到本地磁盘的不同的分区中，且
保证全局有序。因此会涉及到很多并行的任务，且任务之间也有依赖关系。

我们先梳理一下主要的任务以及需要解决的问题:
1. 接收不同 `sinker` 发送的数据，不同的 `sinker` 对应不同的 `table` 以及 `partition`。需要根据 `table` 和
   `partition` 对任务进行分发。
2. 为了提高写入效率，需要多个线程同时写入，即一个 `partition` 对应多个线程, 每个线程写入一个文件且保证文件内有序。
3. 一个 `partition` 结束后，需要启动另一个并发任务来合并文件。由于一个 `partition` 也来自多个不同的 `sinker`
   节点，一个很重要的问题就是如何判断一个 `partition` 结束 ？
4. 合并文件需要另一个并发任务, 为了防止存储节点宕机, 如何保存多副本？

## 读取数据