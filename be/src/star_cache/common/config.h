// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <gflags/gflags.h>

namespace starrocks::starcache::config {

DECLARE_uint64(block_size);
DECLARE_uint64(slice_size);
DECLARE_uint64(block_file_size);

DECLARE_bool(pre_allocate_block_file);
DECLARE_bool(enable_disk_checksum);

DECLARE_uint32(mem_evict_times);
DECLARE_uint32(disk_evict_times);

DECLARE_uint32(max_retry_when_allocate);

DECLARE_uint32(admission_max_check_size);
DECLARE_double(admission_flush_probability);
DECLARE_double(admission_delete_probability);

DECLARE_double(promotion_mem_threshold);
DECLARE_bool(enable_os_page_cache);
DECLARE_uint32(io_align_unit_size);

DECLARE_uint32(access_index_shard_bits);
DECLARE_uint32(sharded_lock_shard_bits);
DECLARE_uint32(lru_container_shard_bits);
    
} // namespace starrocks::starcache
