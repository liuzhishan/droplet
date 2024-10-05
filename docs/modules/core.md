# core

以 `gridbuffer` 格式为基础，实现了一套核心的数据结构以及算法。

`gridbuffer` 地址: [gridbuffer](https://github.com/liuzhishan/gridbuffer)。

有如下一些对于 `gridbuffer` 的操作:

- 对接收到的 `SimpleFeatures` 数据按时间戳进行排序。
- 按行按列过滤。
- 多个 `gridbuffer` 合并。可能是横向合并，也可能是纵向合并，也可能同时有横向纵向。