// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/common/config.h"

namespace starrocks::starcache::config {

DEFINE_uint64(block_size, 1048576, "The size of block, which is the basic unit of cache management"); // 1MB
DEFINE_uint64(slice_size, 65536, "The size of slice, which is the minimum read unit"); // 64KB
DEFINE_uint64(block_file_size, 10737418240,
              "The size of disk block file, which cache a batch of blocks in a disk file"); // 64KB

DEFINE_bool(pre_allocate_block_file, false,
              "Whether to pre-allocate the the block file to the target size");
DEFINE_bool(enable_disk_checksum, true,
              "Whether to do checksum to check the data correctness read from disk");

DEFINE_uint32(mem_evict_times, 2,
              "The times of target size that need to be evicted in an memery eviction");
DEFINE_uint32(disk_evict_times, 2,
              "The times of target size that need to be evicted in a disk eviction");

DEFINE_uint32(max_retry_when_allocate, 10,
              "The maximum retry count when allocate segment or block failed");

DEFINE_uint32(admission_max_check_size, 65536,
              "The max object size that need to be checked by size based admission policy"); // 64KB
DEFINE_double(admission_flush_probability, 1.0,
              "The probability to flush the small block to disk when evict");
DEFINE_double(admission_delete_probability, 1.0,
              "The probability to delete the small block directly when evict");

DEFINE_double(promotion_mem_threshold, 0.8,
              "The threshold under which the promotion is allowed");

DEFINE_bool(enable_os_page_cache, false,
              "Whether to enable os page cache, if not, the disk data will be processed in direct io mode");
//DEFINE_uint32(io_align_unit_size, 512, "The unit size for direct io alignment");
DEFINE_uint32(io_align_unit_size, 4096, "The unit size for direct io alignment");

DEFINE_uint32(access_index_shard_bits, 5, "The shard bits of access index hashmap"); // 32 shards
DEFINE_uint32(sharded_lock_shard_bits, 12, "The shard bits of sharded lock manager"); // 4096 shards
DEFINE_uint32(lru_container_shard_bits, 6, "The shard bits of lru container"); // 64 shards

} // namespace starrocks::starcache
