# 其他格式转换到 `GridSample`

有以下一些格式需要转换到 `GridSample`：

- `SimpleFeatures`
- `GridBuffer`

## `SimpleFeatures` 到 `GridSample`

`SimpleFeatures` 是单条样本，只包含一个 `user` 和一个 `item`, 先转换成 `GridBuffer` 再转换成 `GridSample`。

## `GridBuffer` 到 `GridSample`

有非聚合和聚合两种情况。

### 非聚合

将 `GridBuffer` 直接转换成 `GridSample`, 根据 `GridBuffer` 里的值设置 `GridSample` 中的 `SampleKey`。

### 聚合

聚合分为两种情况: 固定行数与不固定行数。

#### 固定行数

在训练时候 `batch_size` 是固定的，所以经过过滤后的 `GridSample` 的 `rows` 个数需要固定的，必须在构造时候作为参数传递。

#### 不固定行数

将多个 `GridBuffer` 聚合成一个 `GridSample`，由于最终训练还是需要按行进行过滤，所以聚合的 `GridSample` 的 `rows` 个数
也不用固定。可以设置一个最小值即可，在构造时候作为参数传递。