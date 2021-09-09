// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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

    Status read(Slice* result) override;

    Status skip(uint64_t n) override;

    const std::string& filename() const override { return _filename; }

private:
    // Used to store the compressed data read from |_input_file|.
    class CompressedBuffer {
    public:
        explicit CompressedBuffer(size_t buff_size)
                : _compressed_data(BitUtil::round_up(buff_size, CACHELINE_SIZE)), _offset(0), _limit(0) {}

        inline Slice read_buffer() const { return Slice(&_compressed_data[_offset], _limit - _offset); }

        inline Slice write_buffer() const { return Slice(&_compressed_data[_limit], _compressed_data.size() - _limit); }

        inline void skip(size_t n) {
            _offset += n;
            assert(_offset <= _limit);
        }

        inline Status read(SequentialFile* f) {
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
            Status st = f->read(&buff);
            if (st.ok()) {
                if (buff.size == 0) return Status::EndOfFile("read empty from " + f->filename());
                _limit += buff.size;
            }
            return st;
        }

        inline size_t available() const { return _limit - _offset; }

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
