# 导入通用FAQ

## 导入常见问题

### 异常：close index channel failed/too many tablet versions

**问题描述：**

导入频率太快，compaction没能及时合并导致版本数过多，默认版本数1000

**解决方案：**

增大单次导入数据量，降低频率

调整compaction策略，加快合并（调整完需要观察内存和io）,在be.conf中修改以下内容

```plain text
cumulative_compaction_num_threads_per_disk = 4
base_compaction_num_threads_per_disk = 2
cumulative_compaction_check_interval_seconds = 2
```
