# core

以 `gridbuffer` 格式为基础，实现了一套核心的数据结构以及算法。

## `GridBuffer`

`gridbuffer` 地址: [gridbuffer](https://github.com/liuzhishan/gridbuffer)。

有如下一些对于 `gridbuffer` 的操作:

- 对接收到的 `SimpleFeatures` 数据按时间戳进行排序。
- 按行按列过滤。
- 多个 `gridbuffer` 合并。可能是横向合并，也可能是纵向合并，也可能同时有横向纵向。
- 为了加速访问，将列名通过完美 `hash` 映射到 `u32` 的 `id`。

## `GridSample`

## `IDMapping`

为了减少存储以及加速访问，所有的 `col_name` 都映射到 `u32` 的 `id`。采用 `mysql` 的自增 `id` 作为 `col_id`。

`IDMapping` 中通过 `DashMap` 实现 `col_name` 到 `col_id` 的缓存映射。如果 `col_name` 不存在，则向 `mysql`
表插入该 `col_name`， 并得到新的 `col_id`，同时更新缓存。

## `data_converter`

将 `SimpleFeatures` 转换为 `GridSample`。

## `WindowHeap`

`WindowHeap` 是一个基于 `heap` 的窗口，用于对 `GridSample` 进行排序。

## `FeatureInfo`

`FeatureInfo` 用于描述 `SimpleFeatures` 中的每个 `col_name` 的类型。

## `db`

`db` 模块用于访问 `mysql` 数据库。
