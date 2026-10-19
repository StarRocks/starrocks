// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#pragma once

#include <algorithm>
#include <string>

#include "base/string/slice.h"
#include "common/status.h"
#include "storage/sstable/options.h"

namespace starrocks {
class RandomAccessFile;

namespace sstable {

class Block;
struct ReadOptions;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
class BlockHandle {
public:
    // Maximum encoding length of a BlockHandle
    enum { kMaxEncodedLength = 10 + 10 };

    BlockHandle();

    // The offset of the block in the file.
    uint64_t offset() const { return offset_; }
    void set_offset(uint64_t offset) { offset_ = offset; }

    // The size of the stored block
    uint64_t size() const { return size_; }
    void set_size(uint64_t size) { size_ = size; }

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input);

private:
    uint64_t offset_;
    uint64_t size_;
};

// Footer encapsulates the fixed information stored at the tail
// end of every table file.
class Footer {
public:
    // Encoded length of a Footer.  Note that the serialization of a
    // Footer will always occupy exactly this many bytes.  It consists
    // of two block handles and a magic number.
    enum { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

    Footer() = default;

    // The block handle for the metaindex block of the table
    const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
    void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

    // The block handle for the index block of the table
    const BlockHandle& index_handle() const { return index_handle_; }
    void set_index_handle(const BlockHandle& h) { index_handle_ = h; }

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input);

private:
    BlockHandle metaindex_handle_;
    BlockHandle index_handle_;
};

// kTableMagicNumber was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

// 1-byte type + 32-bit crc
static const size_t kBlockTrailerSize = 5;

constexpr size_t kBlockEncodedReadWindow = 64 * 1024;
struct BlockReadMemory {
    size_t encoded = 0;
    size_t decoded = 0;
    char compression = kNoCompression;
    size_t bulk_bytes() const {
        return encoded + kBlockTrailerSize + (compression == kSnappyCompression ? decoded : 0);
    }
    size_t minimum_open_bytes() const {
        return compression == kSnappyCompression
                       ? decoded + std::min(encoded, kBlockEncodedReadWindow) + kBlockTrailerSize
                       : encoded + kBlockTrailerSize;
    }
};

struct BlockContents {
    Slice data;          // Actual contents of data
    bool cachable;       // True iff data can be cached
    bool heap_allocated; // True iff caller should delete[] data.data()
};

// Unverified compression/length hint through the normal file; ReadBlock still validates actual
// input and checksum before exposing decoded output. This performs no bulk allocation.
Status ProbeBlockMemory(RandomAccessFile* file, const BlockHandle& handle, BlockReadMemory* result);

// Read the block identified by "handle" from "file"; expose contents only on success.
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, const BlockHandle& handle, BlockContents* result);

// Implementation details follow.  Clients should ignore,

inline BlockHandle::BlockHandle() : offset_(~static_cast<uint64_t>(0)), size_(~static_cast<uint64_t>(0)) {}

} // namespace sstable
} // namespace starrocks