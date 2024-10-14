// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "storage/sstable/format.h"

#include <snappy/snappy-sinksource.h>
#include <snappy/snappy.h>

#include <string>

#include "common/status.h"
#include "fs/fs.h"
#include "runtime/exec_env.h"
#include "storage/lake/tablet_manager.h"
#include "storage/sstable/coding.h"
#include "util/compression/block_compression.h"
#include "util/crc32c.h"

namespace starrocks::sstable {

void BlockHandle::EncodeTo(std::string* dst) const {
    // Sanity check that all fields have been set
    CHECK(offset_ != ~static_cast<uint64_t>(0));
    CHECK(size_ != ~static_cast<uint64_t>(0));
    PutVarint64(dst, offset_);
    PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
    if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
        return Status::OK();
    } else {
        return Status::Corruption("bad block handle");
    }
}

void Footer::EncodeTo(std::string* dst) const {
    const size_t original_size = dst->size();
    metaindex_handle_.EncodeTo(dst);
    index_handle_.EncodeTo(dst);
    dst->resize(2 * BlockHandle::kMaxEncodedLength); // Padding
    PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
    PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
    assert(dst->size() == original_size + kEncodedLength);
    (void)original_size; // Disable unused variable warning.
}

Status Footer::DecodeFrom(Slice* input) {
    const char* magic_ptr = input->get_data() + kEncodedLength - 8;
    const uint32_t magic_lo = DecodeFixed32(magic_ptr);
    const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
    const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) | (static_cast<uint64_t>(magic_lo)));
    if (magic != kTableMagicNumber) {
        return Status::Corruption("not an sstable (bad magic number)");
    }

    Status result = metaindex_handle_.DecodeFrom(input);
    if (result.ok()) {
        result = index_handle_.DecodeFrom(input);
    }
    if (result.ok()) {
        // We skip over any leftover data (just padding for now) in "input"
        const char* end = magic_ptr + 8;
        *input = Slice(end, input->get_data() + input->get_size() - end);
    }
    return result;
}

/**
 * Decompress the input data using the specified compression algorithm.
 * @param compression_type The compression algorithm to use.
 * @param input The input data to decompress.
 * @param length The length of the input data.
 * @param result The decompressed data.
 * @return Status::OK() if the decompression was successful, an error otherwise.
 */
static Status DecompressBlock(CompressionTypePB compression_type, const char* input, size_t length,
                              BlockContents* result) {
    const BlockCompressionCodec* codec = nullptr;
    // Choose the compression algorithm that we need.
    RETURN_IF_ERROR(get_block_compression_codec(compression_type, &codec));

    // Get decompressed length from the first 64 bits of the input.
    uint64_t output_length = DecodeFixed64(input);

    // Allocate memory for the decompressed data.
    char* output = new char[output_length];

    // Prepare a Slice to hold the decompressed data.
    Slice output_slice(output, output_length);

    // Prepare a Slice for the compressed data, excluding the first 64 bits used for the length.
    Slice input_slice(input + sizeof(uint64_t), length - sizeof(uint64_t));

    // Decompress the input data into the output slice.
    Status status = codec->decompress(input_slice, &output_slice);
    if (!status.ok()) {
        delete[] output;
        return status;
    }

    // Save the decompressed result in the result structure.
    result->data = output_slice;
    result->heap_allocated = true;
    result->cachable = true;
    return Status::OK();
}

Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, const BlockHandle& handle, BlockContents* result) {
    result->data = Slice();
    result->cachable = false;
    result->heap_allocated = false;

    // Read the block contents as well as the type/crc footer.
    // See table_builder.cc for the code that built this structure.
    size_t n = static_cast<size_t>(handle.size());
    char* buf = new char[n + kBlockTrailerSize];
    //Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
    Status s = file->read_at_fully(handle.offset(), buf, n + kBlockTrailerSize);
    ReadIOStat* stat = options.stat;
    if (stat != nullptr) {
        stat->bytes_from_file += (n + kBlockTrailerSize);
    }
    if (!s.ok()) {
        delete[] buf;
        return s;
    }
    Slice contents(buf, n + kBlockTrailerSize);
    if (contents.get_size() != n + kBlockTrailerSize) {
        delete[] buf;
        return Status::Corruption("truncated block read");
    }

    // Check the crc of the type and the block contents
    const char* data = contents.get_data(); // Pointer to where Read put the data
    if (options.verify_checksums) {
        const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
        const uint32_t actual = crc32c::Value(data, n + 1);
        if (actual != crc) {
            delete[] buf;
            s = Status::Corruption("block checksum mismatch");
            return s;
        }
    }

    switch (data[n]) {
    case kNoCompression:
        if (data != buf) {
            // File implementation gave us pointer to some other data.
            // Use it directly under the assumption that it will be live
            // while the file is open.
            delete[] buf;
            result->data = Slice(data, n);
            result->heap_allocated = false;
            result->cachable = false; // Do not double-cache
        } else {
            result->data = Slice(buf, n);
            result->heap_allocated = true;
            result->cachable = true;
        }

        // Ok
        break;
    case kSnappyCompression: {
        size_t ulength = 0;
        if (!snappy::GetUncompressedLength(data, n, &ulength)) {
            delete[] buf;
            return Status::Corruption("corrupted compressed block (snappy) contents");
        }
        char* ubuf = new char[ulength];
        if (!snappy::RawUncompress(data, n, ubuf)) {
            delete[] buf;
            delete[] ubuf;
            return Status::Corruption("corrupted compressed block (snappy) contents");
        }
        delete[] buf;
        result->data = Slice(ubuf, ulength);
        result->heap_allocated = true;
        result->cachable = true;
        break;
    }
    case kLz4FrameCompression: {
        auto st = DecompressBlock(CompressionTypePB::LZ4_FRAME, data, n, result);
        delete[] buf;
        if (!st.ok()) {
            LOG(ERROR) << st.to_string();
            return Status::Corruption("corrupted compressed block (lz4) contents");
        }
        break;
    }
    case kZstdCompression: {
        auto st = DecompressBlock(CompressionTypePB::ZSTD, data, n, result);
        delete[] buf;
        if (!st.ok()) {
            LOG(ERROR) << st.to_string();
            return Status::Corruption("corrupted compressed block (zstd) contents");
        }
        break;
    }
    default:
        std::string error_msg = "bad compression type: " + std::to_string(data[n]);
        delete[] buf;
        LOG(ERROR) << error_msg;
        return Status::Corruption(error_msg);
    }

    return Status::OK();
}

} // namespace starrocks::sstable