// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/util.h"

#include "util/crc32c.h"
#include "util/hash_util.hpp"

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

void* align_buf(const butil::IOBuf& buf) {
    void* aligned_buf = nullptr;
    posix_memalign(&aligned_buf, config::FLAGS_io_align_unit_size, buf.size());

    butil::IOBuf tmp_buf(buf);
    // IOBufCutter is a specialized utility to cut from IOBuf faster than using corresponding
    // methods in IOBuf.
    butil::IOBufCutter cutter(&tmp_buf);
    cutter.cutn(aligned_buf, buf.size());
    return aligned_buf;
}

} // namespace starrocks::starcache
