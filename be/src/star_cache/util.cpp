// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/util.h"

#include "util/crc32c.h"
#include "common/config.h"

namespace starrocks {

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

} // namespace starrocks
