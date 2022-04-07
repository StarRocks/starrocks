// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "env/env.h"
#include "util/bit_util.h"
#include "util/raw_container.h"

namespace starrocks {

class Decompressor;

class CompressedSequentialFile final : public SequentialFile {
public:
    CompressedSequentialFile(std::shared_ptr<SequentialFile> input_file, std::shared_ptr<Decompressor> decompressor,
                             size_t compressed_data_cache_size = 8 * 1024 * 1024LU)
            : _filename("compressed-" + input_file->filename()),
              _input_file(std::move(input_file)),
              _decompressor(std::move(decompressor)),
              _compressed_buff(BitUtil::round_up(compressed_data_cache_size, CACHELINE_SIZE)) {}

    StatusOr<int64_t> read(void* data, int64_t size) override;

    Status skip(uint64_t n) override;

    const std::string& filename() const override { return _filename; }

private:
    // Used to store the compressed data read from |_input_file|.
    class CompressedBuffer {
    public:
        explicit CompressedBuffer(size_t buff_size)
                : _compressed_data(BitUtil::round_up(buff_size, CACHELINE_SIZE)), _offset(0), _limit(0) {}

        Slice read_buffer() const { return Slice(&_compressed_data[_offset], _limit - _offset); }

        Slice write_buffer() const { return Slice(&_compressed_data[_limit], _compressed_data.size() - _limit); }

        void skip(size_t n) {
            _offset += n;
            assert(_offset <= _limit);
        }

        Status read(SequentialFile* f) {
            if (_offset > 0) {
                // Copy the bytes between the buffer's current offset and limit to the beginning of
                // the buffer.
                memmove(&_compressed_data[0], &_compressed_data[_offset], available());
                _limit -= _offset;
                _offset = 0;
            }
            if (_limit >= _compressed_data.size()) {
                return Status::InternalError("reached the buffer limit");
            }
            Slice buff(write_buffer());
            ASSIGN_OR_RETURN(buff.size, f->read(buff.data, buff.size));
            if (buff.size == 0) return Status::EndOfFile("read empty from " + f->filename());
            _limit += buff.size;
            return Status::OK();
        }

        size_t available() const { return _limit - _offset; }

    private:
        raw::RawVector<uint8_t> _compressed_data;
        size_t _offset;
        size_t _limit;
    };

    std::string _filename;
    std::shared_ptr<SequentialFile> _input_file;
    std::shared_ptr<Decompressor> _decompressor;
    CompressedBuffer _compressed_buff;
    bool _stream_end = false;
};

} // namespace starrocks
