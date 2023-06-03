// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

#include "util/compression/block_compression.h"

#include <lz4/lz4.h>
#include <lz4/lz4frame.h>
#include <snappy/snappy-sinksource.h>
#include <snappy/snappy.h>
#include <zlib.h>
#include <zstd/zstd.h>
#include <zstd/zstd_errors.h>

#include "gutil/endian.h"
#include "gutil/strings/substitute.h"
#include "thirdparty/libdeflate/libdeflate.h"
#include "util/compression/compression_context_pool_singletons.h"
#include "util/faststring.h"

namespace starrocks {

// For LZ4F and ZSTD, if page's max compression len >= 10MB, then we will not
// use the compression_buffer.
const static int COMPRESSION_BUFFER_THRESHOLD = 1024 * 1024 * 10;

using strings::Substitute;

Status BlockCompressionCodec::compress(const std::vector<Slice>& inputs, Slice* output, bool use_compression_buffer,
                                       size_t uncompressed_size, faststring* compressed_body1,
                                       raw::RawString* compressed_body2) const {
    if (inputs.size() == 1) {
        return compress(inputs[0], output, use_compression_buffer, uncompressed_size, compressed_body1,
                        compressed_body2);
    }
    faststring buf;
    // we compute total size to avoid more memory copy
    size_t total_size = Slice::compute_total_size(inputs);
    buf.reserve(total_size);
    for (auto& input : inputs) {
        buf.append(input.data, input.size);
    }
    return compress(buf, output, use_compression_buffer, uncompressed_size, compressed_body1, compressed_body2);
}

class Lz4BlockCompression : public BlockCompressionCodec {
public:
    Lz4BlockCompression() : BlockCompressionCodec(CompressionTypePB::LZ4) {}

    Lz4BlockCompression(CompressionTypePB type) : BlockCompressionCodec(type) {}

    static const Lz4BlockCompression* instance() {
        static Lz4BlockCompression s_instance;
        return &s_instance;
    }

    ~Lz4BlockCompression() override = default;

    Status compress(const Slice& input, Slice* output, bool use_compression_buffer, size_t uncompressed_size,
                    faststring* compressed_body1, raw::RawString* compressed_body2) const override {
        return _compress(input, output, use_compression_buffer, uncompressed_size, compressed_body1, compressed_body2);
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

private:
    Status _compress(const Slice& input, Slice* output, bool use_compression_buffer, size_t uncompressed_size,
                     faststring* compressed_body1, raw::RawString* compressed_body2) const {
        StatusOr<compression::LZ4_CCtx_Pool::Ref> ref = compression::getLZ4_CCtx();
        Status status = ref.status();
        if (!status.ok()) {
            return status;
        }
        compression::LZ4CompressContext* context = ref.value().get();
        LZ4_stream_t* ctx = context->ctx;

        [[maybe_unused]] faststring* compression_buffer = nullptr;
        [[maybe_unused]] size_t max_len = 0;
        if (use_compression_buffer) {
            max_len = max_compressed_len(uncompressed_size);
            if (max_len <= COMPRESSION_BUFFER_THRESHOLD) {
                DCHECK_GE(uncompressed_size, 0);
                compression_buffer = &context->compression_buffer;
                compression_buffer->resize(max_len);
                output->data = reinterpret_cast<char*>(compression_buffer->data());
                output->size = max_len;
            } else {
                DCHECK_GE(uncompressed_size, 0);
                if (compressed_body1) {
                    compressed_body1->resize(max_len);
                    output->data = reinterpret_cast<char*>(compressed_body1->data());
                } else {
                    DCHECK(compressed_body2);
                    compressed_body2->resize(max_len);
                    output->data = reinterpret_cast<char*>(compressed_body2->data());
                }
                output->size = max_len;
            }
        }

        int32_t acceleration = 1;
        size_t compressed_size =
                LZ4_compress_fast_continue(ctx, input.data, output->data, input.size, output->size, acceleration);

        if (compressed_size <= 0) {
            context->compression_fail = true;
            return Status::InvalidArgument("Fail to compress LZ4");
        }
        output->size = compressed_size;

        if (use_compression_buffer) {
            if (max_len <= COMPRESSION_BUFFER_THRESHOLD) {
                compression_buffer->resize(output->size);
                if (compressed_body1) {
                    compressed_body1->assign_copy(compression_buffer->data(), compression_buffer->size());
                } else {
                    DCHECK(compressed_body2);
                    compressed_body2->clear();
                    compressed_body2->resize(compression_buffer->size());
                    strings::memcpy_inlined(compressed_body2->data(), compression_buffer->data(),
                                            compression_buffer->size());
                }
                compression_buffer->resize(0);
            } else {
                if (compressed_body1) {
                    compressed_body1->resize(output->size);
                } else {
                    DCHECK(compressed_body2);
                    compressed_body2->resize(output->size);
                }
            }
        }

        return Status::OK();
    }
};

// hadoop-lz4 is not compatible with lz4 CLI.
// hadoop-lz4 uses a block compression scheme on top of lz4.  As per the hadoop
// docs (BlockCompressorStream.java and BlockDecompressorStream.java) the input
// is split into blocks.  Each block "contains the uncompressed length for the
// block followed by one of more length-prefixed blocks of compressed data."
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
    Lz4HadoopBlockCompression() : Lz4BlockCompression(CompressionTypePB::LZ4_HADOOP) {}

    static const Lz4HadoopBlockCompression* instance() {
        static Lz4HadoopBlockCompression s_instance;
        return &s_instance;
    }

    ~Lz4HadoopBlockCompression() override = default;

    Status compress(const Slice& input, Slice* output, bool use_compression_buffer, size_t uncompressed_size,
                    faststring* compressed_body1, raw::RawString* compressed_body2) const override {
        std::vector<Slice> orig_slices;
        orig_slices.emplace_back(input);
        RETURN_IF_ERROR(compress(orig_slices, output, use_compression_buffer, uncompressed_size, compressed_body1,
                                 compressed_body2));
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output, bool use_compression_buffer,
                    size_t uncompressed_size, faststring* compressed_body1,
                    raw::RawString* compressed_body2) const override {
        if (output->size < kHadoopLz4PrefixLength + kHadoopLz4InnerBlockPrefixLength) {
            return Status::InvalidArgument(
                    "Output buffer too small for "
                    "Lz4HadoopBlockCompression::compress");
        }

        auto* output_ptr = output->data + kHadoopLz4PrefixLength;
        auto remaining_output_size = output->size - kHadoopLz4PrefixLength;
        uint32_t decompressed_block_len = 0;

        for (const auto& input : inputs) {
            if (remaining_output_size < kHadoopLz4InnerBlockPrefixLength) {
                return Status::InvalidArgument(
                        "Output buffer too small for "
                        "Lz4HadoopBlockCompression::compress");
            }
            Slice raw_output(output_ptr + kHadoopLz4InnerBlockPrefixLength,
                             remaining_output_size - kHadoopLz4InnerBlockPrefixLength);
            RETURN_IF_ERROR(Lz4BlockCompression::compress(input, &raw_output, use_compression_buffer, uncompressed_size,
                                                          compressed_body1, compressed_body2));

            // Prepend compressed size in bytes to be compatible with Hadoop
            // Lz4Codec
            BigEndian::Store32(output_ptr, static_cast<uint32_t>(raw_output.size));

            output_ptr += raw_output.size + kHadoopLz4InnerBlockPrefixLength;
            remaining_output_size -= raw_output.size + kHadoopLz4InnerBlockPrefixLength;
            decompressed_block_len += input.size;
        }

        // Prepend decompressed size in bytes to be compatible with Hadoop
        // Lz4Codec
        BigEndian::Store32(output->data, decompressed_block_len);

        output->size = output->size - remaining_output_size;

        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) const override {
        auto st = try_decompress(input, output);
        if (st.ok()) {
            return st;
        }

        // some parquet file might be compressed with lz4 while others might be
        // compressed with hadoop-lz4. for compatibility reason, we need to fall
        // back to lz4 if hadoop-lz4 decompress failed.
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
        // of the input buffer two uint32_t's representing (in this order)
        // expected decompressed size in bytes and expected compressed size in
        // bytes.
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
            if (input_len < sizeof(uint32_t)) {
                return Status::InvalidArgument(
                        strings::Substitute("fail to do hadoop-lz4 decompress, input_len=$0", input_len));
            }
            uint32_t decompressed_block_len = BigEndian::Load32(input_ptr);
            input_ptr += sizeof(uint32_t);
            input_len -= sizeof(uint32_t);
            std::size_t remaining_output_size = buffer_size - decompressed_total_len;
            if (remaining_output_size < decompressed_block_len) {
                return Status::InvalidArgument(
                        strings::Substitute("fail to do hadoop-lz4 decompress, "
                                            "remaining_output_size=$0, decompressed_block_len=$1",
                                            remaining_output_size, decompressed_block_len));
            }
            if (input_len <= 0) {
                break;
            }

            do {
                // Check that input length should not be negative.
                if (input_len < sizeof(uint32_t)) {
                    return Status::InvalidArgument(
                            strings::Substitute("fail to do hadoop-lz4 decompress, "
                                                "decompressed_total_len=$0, input_len=$1",
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
                            strings::Substitute("fail to do hadoop-lz4 decompress, "
                                                "decompressed_total_len=$0, "
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
    Lz4fBlockCompression() : BlockCompressionCodec(CompressionTypePB::LZ4_FRAME) {}

    static const Lz4fBlockCompression* instance() {
        static Lz4fBlockCompression s_instance;
        return &s_instance;
    }

    ~Lz4fBlockCompression() override = default;

    Status compress(const Slice& input, Slice* output, bool use_compression_buffer, size_t uncompressed_size,
                    faststring* compressed_body1, raw::RawString* compressed_body2) const override {
        return _compress({input}, output, use_compression_buffer, uncompressed_size, compressed_body1,
                         compressed_body2);
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output, bool use_compression_buffer,
                    size_t uncompressed_size, faststring* compressed_body1,
                    raw::RawString* compressed_body2) const override {
        return _compress(inputs, output, use_compression_buffer, uncompressed_size, compressed_body1, compressed_body2);
    }

    Status decompress(const Slice& input, Slice* output) const override { return _decompress(input, output); }

    size_t max_compressed_len(size_t len) const override {
        return std::max(LZ4F_compressBound(len, &_s_preferences), LZ4F_compressFrameBound(len, &_s_preferences));
    }

private:
    Status _compress(const std::vector<Slice>& inputs, Slice* output, bool use_compression_buffer,
                     size_t uncompressed_size, faststring* compressed_body1, raw::RawString* compressed_body2) const {
        StatusOr<compression::LZ4F_CCtx_Pool::Ref> ref = compression::getLZ4F_CCtx();
        Status status = ref.status();
        if (!status.ok()) {
            return status;
        }
        compression::LZ4FCompressContext* context = ref.value().get();
        LZ4F_compressionContext_t ctx = context->ctx;

        [[maybe_unused]] faststring* compression_buffer = nullptr;
        [[maybe_unused]] size_t max_len = 0;
        if (use_compression_buffer) {
            max_len = max_compressed_len(uncompressed_size);
            if (max_len <= COMPRESSION_BUFFER_THRESHOLD) {
                DCHECK_GE(uncompressed_size, 0);
                compression_buffer = &context->compression_buffer;
                compression_buffer->resize(max_len);
                output->data = reinterpret_cast<char*>(compression_buffer->data());
                output->size = max_len;
            } else {
                DCHECK_GE(uncompressed_size, 0);
                if (compressed_body1) {
                    compressed_body1->resize(max_len);
                    output->data = reinterpret_cast<char*>(compressed_body1->data());
                } else {
                    DCHECK(compressed_body2);
                    compressed_body2->resize(max_len);
                    output->data = reinterpret_cast<char*>(compressed_body2->data());
                }
                output->size = max_len;
            }
        }

        auto wbytes = LZ4F_compressBegin(ctx, output->data, output->size, &_s_preferences);
        if (LZ4F_isError(wbytes)) {
            context->compression_fail = true;
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do LZ4FRAME compress begin, res=$0", LZ4F_getErrorName(wbytes)));
        }
        size_t offset = wbytes;
        for (auto input : inputs) {
            wbytes = LZ4F_compressUpdate(ctx, output->data + offset, output->size - offset, input.data, input.size,
                                         nullptr);
            if (LZ4F_isError(wbytes)) {
                context->compression_fail = true;
                return Status::InvalidArgument(
                        strings::Substitute("Fail to do LZ4FRAME compress update, res=$0", LZ4F_getErrorName(wbytes)));
            }
            offset += wbytes;
        }
        wbytes = LZ4F_compressEnd(ctx, output->data + offset, output->size - offset, nullptr);
        if (LZ4F_isError(wbytes)) {
            context->compression_fail = true;
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do LZ4FRAME compress end, res=$0", LZ4F_getErrorName(wbytes)));
        }
        offset += wbytes;
        output->size = offset;
        if (use_compression_buffer) {
            if (max_len <= COMPRESSION_BUFFER_THRESHOLD) {
                compression_buffer->resize(output->size);
                if (compressed_body1) {
                    compressed_body1->assign_copy(compression_buffer->data(), compression_buffer->size());
                } else {
                    DCHECK(compressed_body2);
                    compressed_body2->clear();
                    compressed_body2->resize(compression_buffer->size());
                    strings::memcpy_inlined(compressed_body2->data(), compression_buffer->data(),
                                            compression_buffer->size());
                }
                compression_buffer->resize(0);
            } else {
                if (compressed_body1) {
                    compressed_body1->resize(output->size);
                } else {
                    DCHECK(compressed_body2);
                    compressed_body2->resize(output->size);
                }
            }
        }
        return Status::OK();
    }

    Status _decompress(const Slice& input, Slice* output) const {
        StatusOr<compression::LZ4F_DCtx_Pool::Ref> ref = compression::getLZ4F_DCtx();
        Status status = ref.status();
        if (!status.ok()) {
            return status;
        }
        compression::LZ4FDecompressContext* context = ref.value().get();
        LZ4F_decompressionContext_t ctx = context->ctx;

        size_t input_size = input.size;
        auto lres = LZ4F_decompress(ctx, output->data, &output->size, input.data, &input_size, nullptr);
        if (LZ4F_isError(lres)) {
            context->decompression_fail = true;
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do LZ4FRAME decompress, res=$0", LZ4F_getErrorName(lres)));
        } else if (input_size != input.size) {
            context->decompression_fail = true;
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do LZ4FRAME decompress: trailing "
                                        "data left in compressed data, "
                                        "read=$0 vs given=$1",
                                        input_size, input.size));
        } else if (lres != 0) {
            context->decompression_fail = true;
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do LZ4FRAME decompress: expect "
                                        "more compressed data, expect=$0",
                                        lres));
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
    explicit SnappySlicesSource(const std::vector<Slice>& slices) {
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
        return _slices[_cur_slice].data + _slice_off;
    }

    // Skip the next n bytes.  Invalidates any buffer returned by
    // a previous call to Peek().
    // REQUIRES: Available() >= n
    void Skip(size_t n) override {
        DCHECK(_available >= n);
        _available -= n;
        while (n > 0) {
            auto left = _slices[_cur_slice].size - _slice_off;
            if (left > n) {
                // n can be digest in current slice
                _slice_off += n;
                return;
            }
            _slice_off = 0;
            _cur_slice++;
            n -= left;
        }
    }

private:
    std::vector<Slice> _slices;
    size_t _available{0};
    size_t _cur_slice{0};
    size_t _slice_off{0};
};

class SnappyBlockCompression : public BlockCompressionCodec {
public:
    SnappyBlockCompression() : BlockCompressionCodec(CompressionTypePB::SNAPPY) {}

    static const SnappyBlockCompression* instance() {
        static SnappyBlockCompression s_instance;
        return &s_instance;
    }

    ~SnappyBlockCompression() override = default;

    Status compress(const Slice& input, Slice* output, bool use_compression_buffer, size_t uncompressed_size,
                    faststring* compressed_body1, raw::RawString* compressed_body2) const override {
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

    Status compress(const std::vector<Slice>& inputs, Slice* output, bool use_compression_buffer,
                    size_t uncompressed_size, faststring* compressed_body1,
                    raw::RawString* compressed_body2) const override {
        SnappySlicesSource source(inputs);
        snappy::UncheckedByteArraySink sink(output->data);
        output->size = snappy::Compress(&source, &sink);
        return Status::OK();
    }

    size_t max_compressed_len(size_t len) const override { return snappy::MaxCompressedLength(len); }
};

class ZlibBlockCompression : public BlockCompressionCodec {
public:
    ZlibBlockCompression() : BlockCompressionCodec(CompressionTypePB::ZLIB) {}

    ZlibBlockCompression(CompressionTypePB type) : BlockCompressionCodec(type) {}

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

    Status compress(const Slice& input, Slice* output, bool use_compression_buffer, size_t uncompressed_size,
                    faststring* compressed_body1, raw::RawString* compressed_body2) const override {
        auto zres = ::compress((Bytef*)output->data, &output->size, (Bytef*)input.data, input.size);
        if (zres != Z_OK) {
            return Status::InvalidArgument(strings::Substitute("Fail to do ZLib compress, error=$0", zError(zres)));
        }
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output, bool use_compression_buffer,
                    size_t uncompressed_size, faststring* compressed_body1,
                    raw::RawString* compressed_body2) const override {
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
        // one-time overhead of six bytes for the entire stream plus five bytes
        // per 16 KB block
        return len + 6 + 5 * ((len >> 14) + 1);
    }
};

class ZstdBlockCompression final : public BlockCompressionCodec {
public:
    ZstdBlockCompression() : BlockCompressionCodec(CompressionTypePB::ZSTD) {}

    static const ZstdBlockCompression* instance() {
        static ZstdBlockCompression s_instance;
        return &s_instance;
    }

    ~ZstdBlockCompression() override = default;

    Status compress(const Slice& input, Slice* output, bool use_compression_buffer, size_t uncompressed_size,
                    faststring* compressed_body1, raw::RawString* compressed_body2) const override {
        return _compress({input}, output, use_compression_buffer, uncompressed_size, compressed_body1,
                         compressed_body2);
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output, bool use_compression_buffer,
                    size_t uncompressed_size, faststring* compressed_body1,
                    raw::RawString* compressed_body2) const override {
        return _compress(inputs, output, use_compression_buffer, uncompressed_size, compressed_body1, compressed_body2);
    }

    Status decompress(const Slice& input, Slice* output) const override { return _decompress(input, output); }

    size_t max_compressed_len(size_t len) const override { return ZSTD_compressBound(len); }

private:
    Status _compress(const std::vector<Slice>& inputs, Slice* output, bool use_compression_buffer,
                     size_t uncompressed_size, faststring* compressed_body1, raw::RawString* compressed_body2) const {
        StatusOr<compression::ZSTD_CCtx_Pool::Ref> ref = compression::getZSTD_CCtx();
        Status status = ref.status();
        if (!status.ok()) {
            return status;
        }
        compression::ZSTDCompressionContext* context = ref.value().get();
        ZSTD_CCtx* ctx = context->ctx;

        [[maybe_unused]] faststring* compression_buffer = nullptr;
        [[maybe_unused]] size_t max_len = 0;
        if (use_compression_buffer) {
            max_len = max_compressed_len(uncompressed_size);
            if (max_len <= COMPRESSION_BUFFER_THRESHOLD) {
                DCHECK_GE(uncompressed_size, 0);
                compression_buffer = &context->compression_buffer;
                compression_buffer->resize(max_len);
                output->data = reinterpret_cast<char*>(compression_buffer->data());
                output->size = max_len;
            } else {
                DCHECK_GE(uncompressed_size, 0);
                if (compressed_body1) {
                    compressed_body1->resize(max_len);
                    output->data = reinterpret_cast<char*>(compressed_body1->data());
                } else {
                    DCHECK(compressed_body2);
                    compressed_body2->resize(max_len);
                    output->data = reinterpret_cast<char*>(compressed_body2->data());
                }
                output->size = max_len;
            }
        }

        ZSTD_outBuffer out_buf;
        out_buf.dst = output->data;
        out_buf.size = output->size;
        out_buf.pos = 0;

        size_t ret;
        for (auto& input : inputs) {
            ZSTD_inBuffer in_buf;
            in_buf.src = input.data;
            in_buf.size = input.size;
            in_buf.pos = 0;

            ret = ZSTD_compressStream(ctx, &out_buf, &in_buf);
            if (ZSTD_isError(ret)) {
                context->compression_fail = true;
                return Status::InvalidArgument(
                        strings::Substitute("ZSTD compress failed: $0", ZSTD_getErrorString(ZSTD_getErrorCode(ret))));
            } else if (input.size != in_buf.pos) {
                context->compression_fail = true;
                return Status::InvalidArgument(strings::Substitute("ZSTD compress failed, buffer is too small"));
            }
        }

        ret = ZSTD_endStream(ctx, &out_buf);
        if (ZSTD_isError(ret)) {
            context->compression_fail = true;
            return Status::InvalidArgument(
                    strings::Substitute("ZSTD compress failed: $0", ZSTD_getErrorString(ZSTD_getErrorCode(ret))));
        } else if (ret > 0) {
            context->compression_fail = true;
            return Status::InvalidArgument(strings::Substitute("ZSTD compress failed, buffer is too small"));
        }
        output->size = out_buf.pos;

        if (use_compression_buffer) {
            if (max_len <= COMPRESSION_BUFFER_THRESHOLD) {
                compression_buffer->resize(output->size);
                if (compressed_body1) {
                    compressed_body1->assign_copy(compression_buffer->data(), compression_buffer->size());
                } else {
                    DCHECK(compressed_body2);
                    compressed_body2->clear();
                    compressed_body2->resize(compression_buffer->size());
                    strings::memcpy_inlined(compressed_body2->data(), compression_buffer->data(),
                                            compression_buffer->size());
                }
                compression_buffer->resize(0);
            } else {
                if (compressed_body1) {
                    compressed_body1->resize(output->size);
                } else {
                    DCHECK(compressed_body2);
                    compressed_body2->resize(output->size);
                }
            }
        }

        return Status::OK();
    }

    Status _decompress(const Slice& input, Slice* output) const {
        StatusOr<compression::ZSTD_DCtx_Pool::Ref> ref = compression::getZSTD_DCtx();
        Status status = ref.status();
        if (!status.ok()) {
            return status;
        }
        compression::ZSTDDecompressContext* context = ref.value().get();
        ZSTD_DCtx* ctx = context->ctx;

        if (output->data == nullptr) {
            // We may pass a NULL 0-byte output buffer but some zstd versions
            // demand a valid pointer:
            // https://github.com/facebook/zstd/issues/1385
            static uint8_t empty_buffer;
            output->data = (char*)&empty_buffer;
            output->size = 0;
        }

        size_t ret = ZSTD_decompressDCtx(ctx, output->data, output->size, input.data, input.size);
        if (ZSTD_isError(ret)) {
            context->decompression_fail = true;
            return Status::InvalidArgument(
                    strings::Substitute("ZSTD compress failed: $0", ZSTD_getErrorString(ZSTD_getErrorCode(ret))));
        }
        output->size = ret;
        return Status::OK();
    }
};

class GzipBlockCompression final : public ZlibBlockCompression {
public:
    GzipBlockCompression() : ZlibBlockCompression(CompressionTypePB::GZIP) {}

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

    Status compress(const Slice& input, Slice* output, bool use_compression_buffer, size_t uncompressed_size,
                    faststring* compressed_body1, raw::RawString* compressed_body2) const override {
        std::vector<Slice> orig_slices;
        orig_slices.emplace_back(input);
        RETURN_IF_ERROR(ZlibBlockCompression::compress(orig_slices, output, use_compression_buffer, uncompressed_size,
                                                       compressed_body1, compressed_body2));
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
            // Fall back to zlib estimate logic for deflate, notice this may
            // cause decompress error
            LOG(WARNING) << strings::Substitute("Fail to do ZLib stream compress, error=$0, res=$1", zError(zres), zres)
                                    .c_str();
            return ZlibBlockCompression::max_compressed_len(len);
        } else {
            zres = deflateEnd(&zstrm);
            if (zres != Z_OK) {
                LOG(WARNING) << strings::Substitute(
                                        "Fail to do deflateEnd on ZLib stream, "
                                        "error=$0, res=$1",
                                        zError(zres), zres)
                                        .c_str();
            }
            // Mark, maintainer of zlib, has stated that 12 needs to be added to
            // result for gzip
            // http://compgroups.net/comp.unix.programmer/gzip-compressing-an-in-memory-string-usi/54854
            // To have a safe upper bound for "wrapper variations", we add 32 to
            // estimate
            int upper_bound = deflateBound(&zstrm, len) + 32;
            return upper_bound;
        }
    }

private:
    // Magic number for zlib, see https://zlib.net/manual.html for more details.
    const static int GZIP_CODEC = 16; // gzip
    // The memLevel parameter specifies how much memory should be allocated for
    // the internal compression state.
    const static int MEM_LEVEL = 8;
};

class GzipBlockCompressionV2 final : public BlockCompressionCodec {
public:
    GzipBlockCompressionV2() : BlockCompressionCodec(CompressionTypePB::GZIP) {
    }

    GzipBlockCompressionV2(CompressionTypePB type) : BlockCompressionCodec(type) {}

    static const GzipBlockCompressionV2* instance() {
        static GzipBlockCompressionV2 s_instance;
        return &s_instance;
    }

    ~GzipBlockCompressionV2() override = default;

    virtual Status init_compress_stream(z_stream& zstrm) const {
        return Status::NotSupported("");
    }

    Status compress(const Slice& input, Slice* output, bool use_compression_buffer, size_t uncompressed_size,
                    faststring* compressed_body1, raw::RawString* compressed_body2) const override {
        return Status::NotSupported("");
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output, bool use_compression_buffer,
                    size_t uncompressed_size, faststring* compressed_body1,
                    raw::RawString* compressed_body2) const override {
        return Status::NotSupported("");
    }

    Status decompress(const Slice& input, Slice* output) const override {
        // 初始化libdeflate解压缩器
        struct libdeflate_decompressor* decompressor = libdeflate_alloc_decompressor();
        if (!decompressor) {
            return Status::InternalError("libdeflate_alloc_decompressor failed");
        }

        // 解压缩数据
        std::size_t out_len;
        auto result = libdeflate_gzip_decompress(decompressor, input.data, input.size, output->data, output->size, &out_len);
        if (result != LIBDEFLATE_SUCCESS) {
            libdeflate_free_decompressor(decompressor);
            return Status::InvalidArgument("libdeflate_gzip_decompress failed");
        }

        libdeflate_free_decompressor(decompressor);

        return Status::OK();
    }

    size_t max_compressed_len(size_t len) const override {
        // one-time overhead of six bytes for the entire stream plus five bytes
        // per 16 KB block
        return len + 6 + 5 * ((len >> 14) + 1);
    }
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
        *codec = GzipBlockCompressionV2::instance();
        break;
    case CompressionTypePB::LZ4_HADOOP:
        *codec = Lz4HadoopBlockCompression::instance();
        break;
    default:
        return Status::NotFound(strings::Substitute("unknown compression type($0)", type));
    }
    return Status::OK();
}

bool use_compression_pool(CompressionTypePB type) {
    if (type == CompressionTypePB::LZ4_FRAME || type == CompressionTypePB::ZSTD || type == CompressionTypePB::LZ4) {
        return true;
    }
    return false;
}

} // namespace starrocks
