// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <butil/iobuf.h>
#include "star_cache/util.h"

namespace starrocks::starcache {

enum class BlockState : uint8_t {
    CLEAN,
    DIRTY,
    EVICTED,
    REMOVED
};

struct BlockSegment {
    uint32_t offset;
    IOBuf buf;

    BlockSegment() {}
    BlockSegment(uint32_t offset_, const IOBuf& buf_)
        : offset(offset_)
        , buf(buf_)
    {}
};

struct MemBlockItem {
    BlockSegment** slices;
    BlockState state;

    MemBlockItem(BlockState state_) : state(state_) {
        const size_t slice_count = block_slice_count();
        slices = new BlockSegment*[slice_count];
        memset(slices, 0, sizeof(BlockSegment*) * slice_count);
    }
    ~MemBlockItem() {
        BlockSegment* cur_segment = nullptr;
        for (size_t i = 0; i < block_slice_count(); ++i) {
           if (slices[i] != cur_segment) {
                delete cur_segment;
                cur_segment = slices[i];
            }
        }
        delete cur_segment;
        delete[] slices;
    }

    void list_segments(std::vector<BlockSegment*> *segments) {
        for (size_t i = 0; i < block_slice_count(); ++i) {
            if (slices[i] && (segments->empty() || slices[i] != segments->back())) {
                segments->push_back(slices[i]);
            }
        }
    }
};

struct DiskBlockItem {
    uint8_t dir_index;
    uint32_t block_index;
    uint32_t* checksums;

    DiskBlockItem(uint8_t dir_index_, uint32_t block_index_)
        : dir_index(dir_index_)
        , block_index(block_index_) {
        checksums = new uint32_t[block_slice_count()];
    }
    ~DiskBlockItem() {
        delete[] checksums;
    }
};

struct BlockItem {
    MemBlockItem* mem_block_item = nullptr;
    DiskBlockItem* disk_block_item = nullptr;

    ~BlockItem() {
        delete mem_block_item;
        delete disk_block_item;
    }
};

} // namespace starrocks::starcache
