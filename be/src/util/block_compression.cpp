// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/block_compression.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/block_compression.h"

#include <lz4/lz4.h>
#include <lz4/lz4frame.h>
#include <snappy/snappy-sinksource.h>
#include <snappy/snappy.h>
#include <zlib.h>
#include <zstd/zstd.h>
#include <zstd/zstd_errors.h>

#include "gutil/endian.h"
#include "gutil/strings/substitute.h"
#include "util/faststring.h"

namespace starrocks {

using strings::Substitute;

Status BlockCompressionCodec::compress(const std::vector<Slice>& inputs, Slice* output) const {
    if (inputs.size() == 1) {
        return compress(inputs[0], output);
    }
    faststring buf;
    // we compute total size to avoid more memory copy
    size_t total_size = Slice::compute_total_size(inputs);
    buf.reserve(total_size);
    for (auto& input : inputs) {
        buf.append(input.data, input.size);
    }
    return compress(buf, output);
}

class Lz4BlockCompression : public BlockCompressionCodec {
public:
    static const Lz4BlockCompression* instance() {
        static Lz4BlockCompression s_instance;
        return &s_instance;
    }

    ~Lz4BlockCompression() override = default;

    Status compress(const Slice& input, Slice* output) const override {
        auto compressed_len = LZ4_compress_default(input.data, output->data, input.size, output->size);
        if (compressed_len == 0) {
            return Status::InvalidArgument(
                    strings::Substitute("Output buffer's capacity is not enough, size=$0", output->size));
        }
        output->size = compressed_len;
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) const override {
        auto decompressed_len = LZ4_decompress_safe(input.data, output->data, input.size, output->size);
        if (decompressed_len < 0) {
            return Status::InvalidArgument(
                    strings::Substitute("fail to do LZ4 decompress, error=$0", decompressed_len));
        }
        output->size = decompressed_len;
        return Status::OK();
    }

    size_t max_compressed_len(size_t len) const override { return LZ4_compressBound(len); }

    bool exceed_max_input_size(size_t len) const override { return len > LZ4_MAX_INPUT_SIZE; }

    size_t max_input_size() const override { return LZ4_MAX_INPUT_SIZE; }
};

// hadoop-lz4 is not compatible with lz4 CLI.
// hadoop-lz4 uses a block compression scheme on top of lz4.  As per the hadoop docs
// (BlockCompressorStream.java and BlockDecompressorStream.java) the input is split
// into blocks.  Each block "contains the uncompressed length for the block followed
// by one of more length-prefixed blocks of compressed data."
// This is essentially blocks of blocks.
// The outer block consists of:
//   - 4 byte big endian uncompressed_size
//   < inner blocks >
//   ... repeated until input_len is consumed ...
// The inner blocks have:
//   - 4-byte big endian compressed_size
//   < lz4 compressed block >
//   - 4-byte big endian compressed_size
//   < lz4 compressed block >
//   ... repeated until uncompressed_size from outer block is consumed ...
//
// the hadoop lz4codec source code can be found here:
// https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask/src/main/native/src/codec/Lz4Codec.cc
//
// More details refer to https://issues.apache.org/jira/browse/HADOOP-12990
class Lz4HadoopBlockCompression : public Lz4BlockCompression {
public:
    static const Lz4HadoopBlockCompression* instance() {
        static Lz4HadoopBlockCompression s_instance;
        return &s_instance;
    }

    ~Lz4HadoopBlockCompression() override = default;

    Status compress(const Slice& input, Slice* output) const override {
        std::vector<Slice> orig_slices;
        orig_slices.emplace_back(input);
        RETURN_IF_ERROR(compress(orig_slices, output));
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output) const override {
        if (output->size < kHadoopLz4PrefixLength + kHadoopLz4InnerBlockPrefixLength) {
            return Status::InvalidArgument("Output buffer too small for Lz4HadoopBlockCompression::compress");
        }

        auto* output_ptr = output->data + kHadoopLz4PrefixLength;
        auto remaining_output_size = output->size - kHadoopLz4PrefixLength;
        uint32_t decompressed_block_len = 0;

        for (const auto& input : inputs) {
            if (remaining_output_size < kHadoopLz4InnerBlockPrefixLength) {
                return Status::InvalidArgument("Output buffer too small for Lz4HadoopBlockCompression::compress");
            }
            Slice raw_output(output_ptr + kHadoopLz4InnerBlockPrefixLength,
                             remaining_output_size - kHadoopLz4InnerBlockPrefixLength);
            RETURN_IF_ERROR(Lz4BlockCompression::compress(input, &raw_output));

            // Prepend compressed size in bytes to be compatible with Hadoop Lz4Codec
            BigEndian::Store32(output_ptr, static_cast<uint32_t>(raw_output.size));

            output_ptr += raw_output.size + kHadoopLz4InnerBlockPrefixLength;
            remaining_output_size -= raw_output.size + kHadoopLz4InnerBlockPrefixLength;
            decompressed_block_len += input.size;
        }

        // Prepend decompressed size in bytes to be compatible with Hadoop Lz4Codec
        BigEndian::Store32(output->data, decompressed_block_len);

        output->size = output->size - remaining_output_size;

        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) const override {
        auto st = try_decompress(input, output);
        if (st.ok()) {
            return st;
        }

        // some parquet file might be compressed with lz4 while others might be compressed with hadoop-lz4.
        // for compatibility reason, we need to fall back to lz4 if hadoop-lz4 decompress failed.
        return Lz4BlockCompression::decompress(input, output);
    }

    // TODO(@DorianZheng) May not enough for multiple input buffers.
    size_t max_compressed_len(size_t len) const override {
        return Lz4BlockCompression::max_compressed_len(len) + kHadoopLz4PrefixLength + kHadoopLz4InnerBlockPrefixLength;
    }

private:
    static const std::size_t kHadoopLz4PrefixLength = sizeof(uint32_t);
    static const std::size_t kHadoopLz4InnerBlockPrefixLength = sizeof(uint32_t);

    Status try_decompress(const Slice& input, Slice* output) const {
        // data written with the hadoop lz4codec contain at the beginning
        // of the input buffer two uint32_t's representing (in this order) expected
        // decompressed size in bytes and expected compressed size in bytes.
        if (input.size < kHadoopLz4PrefixLength + kHadoopLz4InnerBlockPrefixLength) {
            return Status::InvalidArgument(
                    strings::Substitute("fail to do hadoop-lz4 decompress, input size=$0", input.size));
        }

        std::size_t decompressed_total_len = 0;
        const std::size_t buffer_size = output->size;
        auto* input_ptr = input.data;
        auto* out_ptr = output->data;
        std::size_t input_len = input.size;

        while (input_len > 0) {
            uint32_t decompressed_block_len = BigEndian::Load32(input_ptr);
            input_ptr += sizeof(uint32_t);
            input_len -= sizeof(uint32_t);
            std::size_t remaining_output_size = buffer_size - decompressed_total_len;
            if (remaining_output_size < decompressed_block_len) {
                return Status::InvalidArgument(strings::Substitute(
                        "fail to do hadoop-lz4 decompress, remaining_output_size=$0, decompressed_block_len=$1",
                        remaining_output_size, decompressed_block_len));
            }

            do {
                // Check that input length should not be negative.
                if (input_len < 0) {
                    return Status::InvalidArgument(strings::Substitute(
                            "fail to do hadoop-lz4 decompress, decompressed_total_len=$0, input_len=$1",
                            decompressed_total_len, input_len));
                }
                // Read the length of the next lz4 compressed block.
                size_t compressed_len = BigEndian::Load32(input_ptr);
                input_ptr += sizeof(uint32_t);
                input_len -= sizeof(uint32_t);

                if (compressed_len == 0) {
                    continue;
                }

                if (compressed_len > input_len) {
                    return Status::InvalidArgument(
                            strings::Substitute("fail to do hadoop-lz4 decompress, decompressed_total_len=$0, "
                                                "compressed_len=$1, input_len=$2",
                                                decompressed_total_len, compressed_len, input_len));
                }

                // Decompress this block.
                remaining_output_size = buffer_size - decompressed_total_len;
                Slice input_block(input_ptr, compressed_len);
                Slice output_block(out_ptr, remaining_output_size);
                RETURN_IF_ERROR(Lz4BlockCompression::decompress(input_block, &output_block));

                out_ptr += output_block.size;
                input_ptr += compressed_len;
                input_len -= compressed_len;
                decompressed_block_len -= output_block.size;
                decompressed_total_len += output_block.size;
            } while (decompressed_block_len > 0);
        }
        return Status::OK();
    }
};

class Lz4fBlockCompression : public BlockCompressionCodec {
public:
    static const Lz4fBlockCompression* instance() {
        static Lz4fBlockCompression s_instance;
        return &s_instance;
    }

    ~Lz4fBlockCompression() override = default;

    Status compress(const Slice& input, Slice* output) const override {
        auto compressed_len = LZ4F_compressFrame(output->data, output->size, input.data, input.size, &_s_preferences);
        if (LZ4F_isError(compressed_len)) {
            return Status::InvalidArgument(strings::Substitute("Fail to do LZ4FRAME compress frame, msg=$0",
                                                               LZ4F_getErrorName(compressed_len)));
        }
        output->size = compressed_len;
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output) const override {
        LZ4F_compressionContext_t ctx = nullptr;
        auto lres = LZ4F_createCompressionContext(&ctx, LZ4F_VERSION);
        if (lres != 0) {
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do LZ4FRAME compress, res=$0", LZ4F_getErrorName(lres)));
        }
        auto st = _compress(ctx, inputs, output);
        LZ4F_freeCompressionContext(ctx);
        return st;
    }

    Status decompress(const Slice& input, Slice* output) const override {
        LZ4F_decompressionContext_t ctx;
        auto lres = LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION);
        if (LZ4F_isError(lres)) {
            return Status::InvalidArgument(
                    strings::Substitute("Fail to create LZ4FRAME decompress, res=$0", LZ4F_getErrorName(lres)));
        }
        auto st = _decompress(ctx, input, output);
        LZ4F_freeDecompressionContext(ctx);
        return st;
    }

    size_t max_compressed_len(size_t len) const override {
        return std::max(LZ4F_compressBound(len, &_s_preferences), LZ4F_compressFrameBound(len, &_s_preferences));
    }

private:
    Status _compress(LZ4F_compressionContext_t ctx, const std::vector<Slice>& inputs, Slice* output) const {
        auto wbytes = LZ4F_compressBegin(ctx, output->data, output->size, &_s_preferences);
        if (LZ4F_isError(wbytes)) {
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do LZ4FRAME compress begin, res=$0", LZ4F_getErrorName(wbytes)));
        }
        size_t offset = wbytes;
        for (auto input : inputs) {
            wbytes = LZ4F_compressUpdate(ctx, output->data + offset, output->size - offset, input.data, input.size,
                                         nullptr);
            if (LZ4F_isError(wbytes)) {
                return Status::InvalidArgument(
                        strings::Substitute("Fail to do LZ4FRAME compress update, res=$0", LZ4F_getErrorName(wbytes)));
            }
            offset += wbytes;
        }
        wbytes = LZ4F_compressEnd(ctx, output->data + offset, output->size - offset, nullptr);
        if (LZ4F_isError(wbytes)) {
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do LZ4FRAME compress end, res=$0", LZ4F_getErrorName(wbytes)));
        }
        offset += wbytes;
        output->size = offset;
        return Status::OK();
    }

    Status _decompress(LZ4F_decompressionContext_t ctx, const Slice& input, Slice* output) const {
        size_t input_size = input.size;
        auto lres = LZ4F_decompress(ctx, output->data, &output->size, input.data, &input_size, nullptr);
        if (LZ4F_isError(lres)) {
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do LZ4FRAME decompress, res=$0", LZ4F_getErrorName(lres)));
        } else if (input_size != input.size) {
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do LZ4FRAME decompress: trailing data left in compressed data, "
                                        "read=$0 vs given=$1",
                                        input_size, input.size));
        } else if (lres != 0) {
            return Status::InvalidArgument(strings::Substitute(
                    "Fail to do LZ4FRAME decompress: expect more compressed data, expect=$0", lres));
        }
        return Status::OK();
    }

private:
    static LZ4F_preferences_t _s_preferences;
};

LZ4F_preferences_t Lz4fBlockCompression::_s_preferences = {
        {LZ4F_max256KB, LZ4F_blockLinked, LZ4F_noContentChecksum, LZ4F_frame,
         0, // unknown content size,
         0, // 0 == no dictID provided
         LZ4F_noBlockChecksum},
        0,         // compression level; 0 == default
        0,         // autoflush
        1,         // 1: parser favors decompression speed vs compression ratio.
        {0, 0, 0}, // reserved, must be set to 0
};

class SnappySlicesSource : public snappy::Source {
public:
    explicit SnappySlicesSource(const std::vector<Slice>& slices) : _available(0), _cur_slice(0), _slice_off(0) {
        for (auto& slice : slices) {
            // We filter empty slice here to avoid complicated process
            if (slice.size == 0) {
                continue;
            }
            _available += slice.size;
            _slices.push_back(slice);
        }
    }

    ~SnappySlicesSource() override = default;

    // Return the number of bytes left to read from the source
    size_t Available() const override { return _available; }

    // Peek at the next flat region of the source.  Does not reposition
    // the source.  The returned region is empty iff Available()==0.
    //
    // Returns a pointer to the beginning of the region and store its
    // length in *len.
    //
    // The returned region is valid until the next call to Skip() or
    // until this object is destroyed, whichever occurs first.
    //
    // The returned region may be larger than Available() (for example
    // if this ByteSource is a view on a substring of a larger source).
    // The caller is responsible for ensuring that it only reads the
    // Available() bytes.
    const char* Peek(size_t* len) override {
        if (_available == 0) {
            *len = 0;
            return nullptr;
        }
        // we should assure that *len is not 0
        *len = _slices[_cur_slice].size - _slice_off;
        DCHECK(*len != 0);
        return _slices[_cur_slice].data;
    }

    // Skip the next n bytes.  Invalidates any buffer returned by
    // a previous call to Peek().
    // REQUIRES: Available() >= n
    void Skip(size_t n) override {
        _available -= n;
        do {
            auto left = _slices[_cur_slice].size - _slice_off;
            if (left > n) {
                // n can be digest in current slice
                _slice_off += n;
                return;
            }
            _slice_off = 0;
            _cur_slice++;
            n -= left;
        } while (n > 0);
    }

private:
    std::vector<Slice> _slices;
    size_t _available;
    size_t _cur_slice;
    size_t _slice_off;
};

class SnappyBlockCompression : public BlockCompressionCodec {
public:
    static const SnappyBlockCompression* instance() {
        static SnappyBlockCompression s_instance;
        return &s_instance;
    }

    ~SnappyBlockCompression() override = default;

    Status compress(const Slice& input, Slice* output) const override {
        snappy::RawCompress(input.data, input.size, output->data, &output->size);
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) const override {
        if (!snappy::RawUncompress(input.data, input.size, output->data)) {
            return Status::InvalidArgument("Fail to do Snappy decompress");
        }
        // NOTE: GetUncompressedLength only takes O(1) time
        snappy::GetUncompressedLength(input.data, input.size, &output->size);
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output) const override {
        SnappySlicesSource source(inputs);
        snappy::UncheckedByteArraySink sink(output->data);
        output->size = snappy::Compress(&source, &sink);
        return Status::OK();
    }

    size_t max_compressed_len(size_t len) const override { return snappy::MaxCompressedLength(len); }
};

class ZlibBlockCompression : public BlockCompressionCodec {
public:
    static const ZlibBlockCompression* instance() {
        static ZlibBlockCompression s_instance;
        return &s_instance;
    }

    ~ZlibBlockCompression() override = default;

    virtual Status init_compress_stream(z_stream& zstrm) const {
        zstrm.zalloc = Z_NULL;
        zstrm.zfree = Z_NULL;
        zstrm.opaque = Z_NULL;
        auto zres = deflateInit(&zstrm, Z_DEFAULT_COMPRESSION);
        if (zres != Z_OK) {
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do ZLib stream compress, error=$0, res=$1", zError(zres), zres));
        }
        return Status::OK();
    }

    Status compress(const Slice& input, Slice* output) const override {
        auto zres = ::compress((Bytef*)output->data, &output->size, (Bytef*)input.data, input.size);
        if (zres != Z_OK) {
            return Status::InvalidArgument(strings::Substitute("Fail to do ZLib compress, error=$0", zError(zres)));
        }
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output) const override {
        z_stream zstrm;
        RETURN_IF_ERROR(init_compress_stream(zstrm));
        // we assume that output is e
        zstrm.next_out = (Bytef*)output->data;
        zstrm.avail_out = output->size;
        auto zres = Z_OK;
        for (int i = 0; i < inputs.size(); ++i) {
            if (inputs[i].size == 0) {
                continue;
            }
            zstrm.next_in = (Bytef*)inputs[i].data;
            zstrm.avail_in = inputs[i].size;
            int flush = (i == (inputs.size() - 1)) ? Z_FINISH : Z_NO_FLUSH;

            zres = deflate(&zstrm, flush);
            if (zres != Z_OK && zres != Z_STREAM_END) {
                return Status::InvalidArgument(
                        strings::Substitute("Fail to do ZLib stream compress, error=$0, res=$1", zError(zres), zres));
            }
        }

        output->size = zstrm.total_out;
        zres = deflateEnd(&zstrm);
        if (zres != Z_OK) {
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do deflateEnd on ZLib stream, error=$0, res=$1", zError(zres), zres));
        }
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) const override {
        size_t input_size = input.size;
        auto zres = ::uncompress2((Bytef*)output->data, &output->size, (Bytef*)input.data, &input_size);
        if (zres != Z_OK) {
            return Status::InvalidArgument(strings::Substitute("Fail to do ZLib decompress, error=$0", zError(zres)));
        }
        return Status::OK();
    }

    size_t max_compressed_len(size_t len) const override {
        // one-time overhead of six bytes for the entire stream plus five bytes per 16 KB block
        return len + 6 + 5 * ((len >> 14) + 1);
    }
};

class ZstdBlockCompression final : public BlockCompressionCodec {
public:
    static const ZstdBlockCompression* instance() {
        static ZstdBlockCompression s_instance;
        return &s_instance;
    }

    ~ZstdBlockCompression() override = default;

    Status compress(const Slice& input, Slice* output) const override {
        size_t ret = ZSTD_compress(output->data, output->size, input.data, input.size, ZSTD_CLEVEL_DEFAULT);
        if (ZSTD_isError(ret)) {
            return Status::InvalidArgument(
                    strings::Substitute("ZSTD compress failed: $0", ZSTD_getErrorString(ZSTD_getErrorCode(ret))));
        }
        output->size = ret;
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output) const override {
        auto deleter = [](ZSTD_CStream* s) {
            if (s != nullptr) ZSTD_freeCStream(s);
        };
        std::unique_ptr<ZSTD_CStream, decltype(deleter)> stream{ZSTD_createCStream(), deleter};
        auto ret = ZSTD_initCStream(stream.get(), ZSTD_CLEVEL_DEFAULT);
        if (ZSTD_isError(ret)) {
            return Status::InvalidArgument(strings::Substitute("ZSTD create compress stream failed: $0", ret));
        }

        ZSTD_outBuffer out_buf;
        out_buf.dst = output->data;
        out_buf.size = output->size;
        out_buf.pos = 0;

        for (auto& input : inputs) {
            ZSTD_inBuffer in_buf;
            in_buf.src = input.data;
            in_buf.size = input.size;
            in_buf.pos = 0;

            ret = ZSTD_compressStream(stream.get(), &out_buf, &in_buf);
            if (ZSTD_isError(ret)) {
                return Status::InvalidArgument(
                        strings::Substitute("ZSTD compress failed: $0", ZSTD_getErrorString(ZSTD_getErrorCode(ret))));
            } else if (input.size != in_buf.pos) {
                return Status::InvalidArgument(strings::Substitute("ZSTD compress failed, buffer is too small"));
            }
        }

        ret = ZSTD_endStream(stream.get(), &out_buf);
        if (ZSTD_isError(ret)) {
            return Status::InvalidArgument(
                    strings::Substitute("ZSTD compress failed: $0", ZSTD_getErrorString(ZSTD_getErrorCode(ret))));
        } else if (ret > 0) {
            return Status::InvalidArgument(strings::Substitute("ZSTD compress failed, buffer is too small"));
        }
        output->size = out_buf.pos;
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) const override {
        if (output->data == nullptr) {
            // We may pass a NULL 0-byte output buffer but some zstd versions demand
            // a valid pointer: https://github.com/facebook/zstd/issues/1385
            static uint8_t empty_buffer;
            output->data = (char*)&empty_buffer;
            output->size = 0;
        }
        size_t ret = ZSTD_decompress(output->data, output->size, input.data, input.size);
        if (ZSTD_isError(ret)) {
            return Status::InvalidArgument(
                    strings::Substitute("ZSTD compress failed: $0", ZSTD_getErrorString(ZSTD_getErrorCode(ret))));
        }
        output->size = ret;
        return Status::OK();
    }

    size_t max_compressed_len(size_t len) const override { return ZSTD_compressBound(len); }
};

class GzipBlockCompression final : public ZlibBlockCompression {
public:
    static const GzipBlockCompression* instance() {
        static GzipBlockCompression s_instance;
        return &s_instance;
    }

    ~GzipBlockCompression() override = default;

    Status init_compress_stream(z_stream& zstrm) const override {
        zstrm.zalloc = Z_NULL;
        zstrm.zfree = Z_NULL;
        zstrm.opaque = Z_NULL;
        auto zres = deflateInit2(&zstrm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + GZIP_CODEC, MEM_LEVEL,
                                 Z_DEFAULT_STRATEGY);
        if (zres != Z_OK) {
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do ZLib stream compress, error=$0, res=$1", zError(zres), zres));
        }
        return Status::OK();
    }

    Status compress(const Slice& input, Slice* output) const override {
        std::vector<Slice> orig_slices;
        orig_slices.emplace_back(input);
        RETURN_IF_ERROR(ZlibBlockCompression::compress(orig_slices, output));
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) const override {
        z_stream z_strm = {nullptr};
        z_strm.zalloc = Z_NULL;
        z_strm.zfree = Z_NULL;
        z_strm.opaque = Z_NULL;

        int ret = inflateInit2(&z_strm, MAX_WBITS + GZIP_CODEC);
        if (ret != Z_OK) {
            return Status::InternalError(
                    strings::Substitute("Fail to do ZLib stream compress, error=$0, res=$1", zError(ret), ret));
        }

        // 1. set input and output
        z_strm.next_in = reinterpret_cast<Bytef*>(input.data);
        z_strm.avail_in = input.size;
        z_strm.next_out = reinterpret_cast<Bytef*>(output->data);
        z_strm.avail_out = output->size;

        if (z_strm.avail_out > 0) {
            // We only support non-streaming use case  for block decompressor
            ret = inflate(&z_strm, Z_FINISH);
            VLOG(10) << "gzip dec ret: " << ret;
            if (ret != Z_OK && ret != Z_STREAM_END) {
                (void)inflateEnd(&z_strm);
                return Status::InternalError(
                        strings::Substitute("Fail to do ZLib stream compress, error=$0, res=$1", zError(ret), ret));
            }
        }
        (void)inflateEnd(&z_strm);

        return Status::OK();
    }

    size_t max_compressed_len(size_t len) const override {
        z_stream zstrm;
        zstrm.zalloc = Z_NULL;
        zstrm.zfree = Z_NULL;
        zstrm.opaque = Z_NULL;
        auto zres = deflateInit2(&zstrm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + GZIP_CODEC, MEM_LEVEL,
                                 Z_DEFAULT_STRATEGY);
        if (zres != Z_OK) {
            // Fall back to zlib estimate logic for deflate, notice this may cause decompress error
            LOG(WARNING) << strings::Substitute("Fail to do ZLib stream compress, error=$0, res=$1", zError(zres), zres)
                                    .c_str();
            return ZlibBlockCompression::max_compressed_len(len);
        } else {
            zres = deflateEnd(&zstrm);
            if (zres != Z_OK) {
                LOG(WARNING) << strings::Substitute("Fail to do deflateEnd on ZLib stream, error=$0, res=$1",
                                                    zError(zres), zres)
                                        .c_str();
            }
            // Mark, maintainer of zlib, has stated that 12 needs to be added to result for gzip
            // http://compgroups.net/comp.unix.programmer/gzip-compressing-an-in-memory-string-usi/54854
            // To have a safe upper bound for "wrapper variations", we add 32 to estimate
            int upper_bound = deflateBound(&zstrm, len) + 32;
            return upper_bound;
        }
    }

private:
    // Magic number for zlib, see https://zlib.net/manual.html for more details.
    const static int GZIP_CODEC = 16; // gzip
    // The memLevel parameter specifies how much memory should be allocated for the internal compression state.
    const static int MEM_LEVEL = 8;
};

Status get_block_compression_codec(CompressionTypePB type, const BlockCompressionCodec** codec) {
    switch (type) {
    case CompressionTypePB::NO_COMPRESSION:
        *codec = nullptr;
        break;
    case CompressionTypePB::SNAPPY:
        *codec = SnappyBlockCompression::instance();
        break;
    case CompressionTypePB::LZ4:
        *codec = Lz4BlockCompression::instance();
        break;
    case CompressionTypePB::LZ4_FRAME:
        *codec = Lz4fBlockCompression::instance();
        break;
    case CompressionTypePB::ZLIB:
        *codec = ZlibBlockCompression::instance();
        break;
    case CompressionTypePB::ZSTD:
        *codec = ZstdBlockCompression::instance();
        break;
    case CompressionTypePB::GZIP:
        *codec = GzipBlockCompression::instance();
        break;
    case CompressionTypePB::LZ4_HADOOP:
        *codec = Lz4HadoopBlockCompression::instance();
        break;
    default:
        return Status::NotFound(strings::Substitute("unknown compression type($0)", type));
    }
    return Status::OK();
}

} // namespace starrocks
