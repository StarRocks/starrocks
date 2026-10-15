// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "storage/sstable/format.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <string>

#include "base/compression/compression_headers.h"
#include "base/debug/trace.h"
#include "base/hash/crc32c.h"
#include "common/status.h"
#include "fs/fs.h"
#include "storage/sstable/coding.h"

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

Status ProbeBlockMemory(RandomAccessFile* file, const BlockHandle& handle, BlockReadMemory* result) {
    if (handle.size() > std::numeric_limits<size_t>::max() - kBlockTrailerSize ||
        handle.offset() > std::numeric_limits<uint64_t>::max() - handle.size() - kBlockTrailerSize) {
        return Status::Corruption("SST block extent overflows");
    }
    result->encoded = handle.size();
    RETURN_IF_ERROR(file->read_at_fully(handle.offset() + handle.size(), &result->compression, 1));
    result->decoded = result->encoded;
    if (result->compression == kSnappyCompression) {
        char prefix[5];
        const size_t size = std::min(result->encoded, sizeof(prefix));
        RETURN_IF_ERROR(file->read_at_fully(handle.offset(), prefix, size));
        if (!snappy::GetUncompressedLength(prefix, size, &result->decoded) ||
            result->decoded > std::numeric_limits<size_t>::max() - result->encoded - kBlockTrailerSize) {
            return Status::Corruption("invalid SST block uncompressed length");
        }
    } else if (result->compression != kNoCompression) {
        return Status::Corruption("bad block type");
    }
    return Status::OK();
}

namespace {
constexpr size_t kEncodedWindow = kBlockEncodedReadWindow;

// Bound encoded input through the same (possibly decrypting) RandomAccessFile.
class BlockSource final : public snappy::Source {
public:
    BlockSource(RandomAccessFile* file, uint64_t offset, size_t size, const ReadOptions& options)
            : _file(file),
              _offset(offset),
              _remaining(size),
              _capacity(std::min(size, kEncodedWindow)),
              _buffer(new char[_capacity]),
              _options(options) {}
    size_t Available() const override { return _status.ok() ? _remaining : 0; }
    const char* Peek(size_t* size) override {
        if (_begin == _end && Available()) {
            const size_t n = std::min(_remaining, _capacity);
            if (_options.check_read_cancelled) _status = _options.check_read_cancelled();
            if (_status.ok()) {
                _status = _file->read_at_fully(_offset, _buffer.get(), n);
                if (_options.stat) _options.stat->bytes_from_file += n;
            }
            if (_status.ok()) {
                _offset += n;
                _begin = 0;
                _end = n;
                if (_options.verify_checksums) _crc = crc32c::Extend(_crc, _buffer.get(), n);
            }
        }
        *size = _status.ok() ? _end - _begin : 0;
        return _buffer.get() + _begin;
    }
    void Skip(size_t n) override {
        DCHECK_LE(n, Available());
        while (n && _status.ok()) {
            size_t ready;
            Peek(&ready);
            if (!ready) break;
            const size_t step = std::min(n, ready);
            _begin += step;
            _remaining -= step;
            n -= step;
        }
    }
    Status drain() {
        while (Available()) {
            size_t n;
            Peek(&n);
            if (!n) break;
            Skip(n);
        }
        return _status;
    }
    void rewind(uint64_t offset, size_t size) {
        _offset = offset;
        _remaining = size;
        _begin = _end = 0;
        _crc = 0;
        _status = Status::OK();
    }
    const Status& status() const { return _status; }
    uint32_t crc() const { return _crc; }

private:
    RandomAccessFile* _file;
    uint64_t _offset;
    size_t _remaining;
    size_t _capacity;
    std::unique_ptr<char[]> _buffer;
    const ReadOptions& _options;
    size_t _begin = 0;
    size_t _end = 0;
    uint32_t _crc = 0;
    Status _status;
};

Status read_snappy_bounded(RandomAccessFile* file, const ReadOptions& options, const BlockHandle& handle,
                           BlockContents* result) {
    TRACE_COUNTER_SCOPE_LATENCY_US("sst_encoded_input_window_read_us");
    const size_t n = handle.size();
    BlockSource source(file, handle.offset(), n, options);
    size_t first_size;
    const char* first = source.Peek(&first_size);
    RETURN_IF_ERROR(source.status());
    size_t actual_decoded = 0;
    const bool prefix_valid = snappy::GetUncompressedLength(first, first_size, &actual_decoded);
    auto drain_and_verify = [&]() -> Status {
        RETURN_IF_ERROR(source.drain());
        char trailer[kBlockTrailerSize];
        if (options.check_read_cancelled) RETURN_IF_ERROR(options.check_read_cancelled());
        const auto st = file->read_at_fully(handle.offset() + n, trailer, sizeof(trailer));
        if (options.stat) options.stat->bytes_from_file += sizeof(trailer);
        RETURN_IF_ERROR(st);
        if (options.verify_checksums &&
            crc32c::Extend(source.crc(), trailer, 1) != crc32c::Unmask(DecodeFixed32(trailer + 1))) {
            return Status::Corruption("block checksum mismatch");
        }
        if (trailer[0] != kSnappyCompression) return Status::Corruption("bad block type");
        return Status::OK();
    };
    if (!prefix_valid) {
        RETURN_IF_ERROR(drain_and_verify());
        return Status::Corruption("corrupted compressed block contents");
    }
    // Reserve only the fixed input window before reading the actual prefix. A corrupt tiny
    // probe must never force an oversized output allocation or hide an IO/checksum error as
    // memory pressure. If output does not fit immediately, validate the encoded stream before
    // waiting/rejecting, then rewind the same input buffer and confirm its stable prefix.
    if (!options.try_reserve_memory(actual_decoded)) {
        RETURN_IF_ERROR(drain_and_verify());
        RETURN_IF_ERROR(options.reserve_memory(actual_decoded));
        source.rewind(handle.offset(), n);
        first = source.Peek(&first_size);
        RETURN_IF_ERROR(source.status());
        size_t confirmed = 0;
        if (!snappy::GetUncompressedLength(first, first_size, &confirmed) || confirmed != actual_decoded) {
            RETURN_IF_ERROR(drain_and_verify());
            return Status::Corruption("SST block changed during read");
        }
    }
    TRACE_COUNTER_INCREMENT("sst_encoded_input_window_reservation_bytes", actual_decoded);
    // RawUncompress sees this very same cached first chunk; its output limit was validated.
    std::unique_ptr<char[]> output(new char[actual_decoded]);
    const bool decoded = snappy::RawUncompress(&source, output.get());
    // IO/checksum errors retain precedence even if decoding stops early on malformed data.
    RETURN_IF_ERROR(drain_and_verify());
    if (!decoded) return Status::Corruption("corrupted compressed block contents");
    result->data = Slice(output.release(), actual_decoded);
    result->heap_allocated = true;
    result->cachable = true;
    return Status::OK();
}
} // namespace

Status ReadBlock(RandomAccessFile* input_file, const ReadOptions& options, const BlockHandle& handle,
                 BlockContents* result) {
    RandomAccessFile* file = nullptr;
    if (options.file != nullptr) {
        // use specified file.
        file = options.file;
    } else {
        file = input_file;
    }
    result->data = Slice();
    result->cachable = false;
    result->heap_allocated = false;

    // Read the block contents as well as the type/crc footer.
    // See table_builder.cc for the code that built this structure.
    size_t n = static_cast<size_t>(handle.size());
    size_t reserved = 0;
    bool bounded_snappy = false;
    if (options.reserve_memory) {
        BlockReadMemory memory;
        RETURN_IF_ERROR(ProbeBlockMemory(file, handle, &memory));
        const auto compression = memory.compression;
        const size_t decoded = memory.decoded;
        size_t bytes = memory.bulk_bytes();
        if (compression == kSnappyCompression) {
            if (n > kEncodedWindow && options.try_reserve_memory) {
                if (options.try_reserve_memory(bytes)) {
                    reserved = bytes;
                } else {
                    bounded_snappy = true;
                    bytes = std::min(n, kEncodedWindow) + kBlockTrailerSize;
                    TRACE_COUNTER_INCREMENT("sst_encoded_input_window_count", 1);
                    TRACE_COUNTER_INCREMENT("sst_encoded_input_window_reservation_bytes", bytes);
                }
            }
        }
        if (options.is_index_block) {
            // These probes use the normal (possibly decrypting) RandomAccessFile, before bulk
            // allocation. Encoded+decoded overlap is an open peak, not the decoded index size.
            // Counters are sums across opens. A per-file size needs one probe or separate file metadata.
            TRACE_COUNTER_INCREMENT("sst_index_probe_count", 1);
            TRACE_COUNTER_INCREMENT("sst_index_probe_snappy_count", compression == kSnappyCompression);
            TRACE_COUNTER_INCREMENT("sst_index_probe_encoded_bytes", n);
            TRACE_COUNTER_INCREMENT("sst_index_probe_decoded_bytes", decoded);
            TRACE_COUNTER_INCREMENT("sst_index_probe_open_peak_bytes",
                                    n + kBlockTrailerSize + (compression == kSnappyCompression ? decoded : 0));
        }
        if (reserved == 0) {
            RETURN_IF_ERROR(options.reserve_memory(bytes));
            reserved = bytes;
        }
    }
    if (bounded_snappy) {
        return read_snappy_bounded(file, options, handle, result);
    }
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
            return Status::Corruption("corrupted compressed block contents");
        }
        if (options.reserve_memory) {
            // Validate the bytes actually read as well. Corrupt cache data must not turn a
            // smaller header probe into an unreserved decompression allocation.
            if (ulength > std::numeric_limits<size_t>::max() - n - kBlockTrailerSize) {
                delete[] buf;
                return Status::Corruption("SST uncompressed block size overflows");
            }
            const size_t actual = n + kBlockTrailerSize + ulength;
            if (actual > reserved) {
                auto status = options.reserve_memory(actual - reserved);
                if (!status.ok()) {
                    delete[] buf;
                    return status;
                }
            }
        }
        char* ubuf = new char[ulength];
        if (!snappy::RawUncompress(data, n, ubuf)) {
            delete[] buf;
            delete[] ubuf;
            return Status::Corruption("corrupted compressed block contents");
        }
        delete[] buf;
        result->data = Slice(ubuf, ulength);
        result->heap_allocated = true;
        result->cachable = true;
        break;
    }
    default:
        delete[] buf;
        return Status::Corruption("bad block type");
    }

    return Status::OK();
}

} // namespace starrocks::sstable
