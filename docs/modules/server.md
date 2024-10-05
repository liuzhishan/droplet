# server

`server` 启动时候需要向 `meta_server` 注册自己，注册成功后，`server` 会定期向 `meta_server` 发送心跳包，
如果 `meta_server` 长时间收不到 `server` 的心跳包，则认为 `server` 已经挂掉。
