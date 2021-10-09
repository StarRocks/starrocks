# 部署问题

## 集群重启时，fe启动失败报错：Fe type:unknown ,is ready :false

确认master是否已启动，或者尝试逐台重启。

## 安装集群报错：failed to get service info err

检查机器是否开启了sshd。`/etc/init.d/sshd status`查看sshd状态。

## BE启动失败，日志报错：Fail to get master client from cache. host= port=0 code=THRIFT_RPC_ERROR

检查be.conf中的端口是否占用，`netstat  -anp  |grep  port`查看是否占用，更换其他空闲端口后重启。

## 企业版升级Manager时，提示：Failed to transport upgrade files to agent host. src:…

检查对应的磁盘，看是否空间不足。因为在集群升级时，Manager会将新版本的二进制文件分发至各个节点，若部署目录的磁盘空间不足，就无法完成文件分发，出现上述报错。

## 新扩容节点的FE状态正常，但是Manager"诊断"页面下该FE节点日志展示报错："Failed to search log."

Manager默认30秒内去获取新部署FE的路径配置，如果FE启动较慢或由于其他原因导致30s内未响应就会出现上述问题。检查Manager Web日志，日志目录例如：`/starrocks-manager-xxx/center/log/webcenter/log/web/drms.INFO`，搜索日志是否有：`Failed to update fe configurations`，若有，重启对应的FE服务。重启会重新获取路径配置。
