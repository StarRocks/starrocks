// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/utils.h"

#include "util/crc32c.h"
#include "util/hash_util.hpp"
#include "util/bit_util.h"

namespace starrocks::starcache {

uint32_t crc32(const butil::IOBuf& buf) {
    uint32_t hash = 0;
    const size_t block_num = buf.backing_block_num();
    for (size_t i = 0; i < block_num; ++i) {
        auto sp = buf.backing_block(i);
        if (!sp.empty()) {
            hash = crc32c::Extend(hash, sp.data(), sp.size());
        }
    }
    return hash;
}

uint64_t cachekey2id(const std::string& key) {
    return HashUtil::hash64(key.data(), key.size(), 0);
}

size_t align_buf(const butil::IOBuf& buf, void** aligned_data) {
    size_t aligned_unit = config::FLAGS_io_align_unit_size;
    size_t aligned_size = starrocks::BitUtil::round_up(buf.size(), aligned_unit);
    posix_memalign(aligned_data, aligned_unit, aligned_size);

    butil::IOBuf tmp_buf(buf);
    // IOBufCutter is a specialized utility to cut from IOBuf faster than using corresponding
    // methods in IOBuf.
    butil::IOBufCutter cutter(&tmp_buf);
    cutter.cutn(*aligned_data, buf.size());
    return aligned_size;
}

} // namespace starrocks::starcache
