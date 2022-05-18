# Loading FAQs

## Common issues in loading

### Error：close index channel failed/too many tablet versions

**Issue description:**

Frequent imports and failure to merge compaction result in too many versions. The default number is 1000.

**Solution:**

Increase data load for one-time import and decrease import frequency.

Adjust compaction strategy and speed up compaction (memory and io should be carefully studied after adjustment). And make the following changes to be.conf:

```plain text
cumulative_compaction_num_threads_per_disk = 4
base_compaction_num_threads_per_disk = 2
cumulative_compaction_check_interval_seconds = 2
```
