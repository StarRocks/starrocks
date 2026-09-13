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

#pragma once

#include <stddef.h>

#include <cstdint>
#include <string_view>

#include "base/string/slice.h"
#include "cache/mem_cache/page_handle.h"
#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/parquet_types.h"
#include "io/seekable_input_stream.h"

namespace starrocks {
class ObjectCache;
class StoragePageCache;
class BlockCompressionCodec;
} // namespace starrocks

// Apache parquet-cpp Parquet Modular Encryption primitives (forward declarations so
// the heavy internal headers stay confined to page_reader.cpp).
namespace parquet {
class InternalFileDecryptor;
class Decryptor;
class FileDecryptionProperties;
} // namespace parquet

namespace starrocks::parquet {

struct ColumnReaderOptions;

// Used to parse page header of column chunk. This class don't parse page's type.
class PageReader {
public:
    PageReader(io::SeekableInputStream* stream, size_t start, size_t length, size_t num_values,
               const ColumnReaderOptions& opts, const tparquet::CompressionCodec::type codec,
               int16_t column_ordinal = 0);

    ~PageReader();

    // Try to parse header starts from current _offset. Caller should assure that
    // _offset locates at the start of page header. If _offset doesn't locate the
    // start of header, this function will return error.
    // return Stats::OK if parse page header success.
    Status next_header();

    const tparquet::PageHeader* current_header() const { return &_cur_header; }

    // seek to read position, this position must be a start of a page header.
    Status seek_to_offset(uint64_t offset) {
        _offset = offset;
        _next_header_pos = offset;
        return _stream->seek(offset);
    }

    Status next_page();

    bool is_last_page() { return _num_values_read >= _num_values_total || _next_read_page_idx >= _page_num; }

    void set_page_num(size_t page_num) { _page_num = page_num; }

    // Tell the reader whether this column chunk begins with a dictionary page; needed
    // to choose the correct PME module type/AAD for the first page before it can be
    // decrypted (the page type is only known after decryption).
    void set_has_dictionary_page(bool v) { _has_dictionary_page = v; }

    void set_next_read_page_idx(size_t cur_page_idx) { _next_read_page_idx = cur_page_idx; }

    StatusOr<Slice> read_and_decompress_page_data();

private:
    // Must call this function after next_header called. The total read size
    // after one next_header can not exceed the page's compressed_page_size.
    Status _read_bytes(void* buffer, size_t size);

    Status _skip_bytes(size_t size);

    StatusOr<std::string_view> _peek(size_t size);

    int32_t _data_length() {
        // For an encrypted file, compressed_page_size is the on-disk (encrypted) module
        // size regardless of codec; the plaintext size emerges after decryption.
        if (_encrypted) {
            return _cur_header.compressed_page_size;
        }
        return _codec != tparquet::CompressionCodec::UNCOMPRESSED ? _cur_header.compressed_page_size
                                                                  : _cur_header.uncompressed_page_size;
    }

    void _init_page_cache_key();
    std::string& _current_page_cache_key();
    Status _deal_page_with_cache();
    Status _read_and_deserialize_header(bool need_fill_cache);
    Status _read_and_decompress_internal(bool need_fill_cache);
    bool _cache_decompressed_data();
    Status _decompress_page(Slice& input, Slice* output);

    // Parquet Modular Encryption (PME). Built once when the file's footer was
    // encrypted; each PageReader owns its own decryptors (the parquet Decryptor
    // mutates AAD per page and must not be shared across column readers).
    Status _init_decryption();
    // Read and decrypt the (length-prefixed) encrypted page header module, then
    // deserialize the plaintext into _cur_header. Used instead of the incremental
    // peek path for encrypted files.
    Status _read_and_decrypt_header();
    // Decrypt one encrypted page-data module (ciphertext) into _decrypt_buf and
    // return the plaintext (still-compressed) slice.
    StatusOr<Slice> _decrypt_page_module(const uint8_t* ciphertext, size_t ciphertext_len);

    io::SeekableInputStream* const _stream;
    tparquet::PageHeader _cur_header;
    uint32_t _header_length;

    uint64_t _offset = 0;
    uint64_t _next_header_pos = 0;
    const uint64_t _finish_offset = 0;

    uint64_t _num_values_read = 0;
    const uint64_t _num_values_total = 0;
    const ColumnReaderOptions& _opts;

    size_t _page_num = 0xffffffff;
    size_t _next_read_page_idx = 0;

    StoragePageCache* _cache = nullptr;
    std::string _page_cache_key;

    const tparquet::CompressionCodec::type _codec;
    const BlockCompressionCodec* _compress_codec = nullptr;

    using BufferPtr = std::unique_ptr<std::vector<uint8_t>>;
    BufferPtr _compressed_buf;
    BufferPtr _uncompressed_buf;
    std::vector<uint8_t>* _cache_buf = nullptr;
    PageHandle _page_handle;
    bool _hit_cache = false;
    bool _skip_page_cache = false;

    Slice _uncompressed_data;

    // Parquet Modular Encryption state (only used when _encrypted is true).
    bool _encrypted = false;
    Status _encryption_status; // result of _init_decryption(), checked before first use
    int16_t _column_ordinal = 0;
    bool _has_dictionary_page = false; // set by ColumnChunkReader from chunk metadata
    bool _dict_page_read = false;      // the dictionary page (if any) is the first page
    bool _cur_is_dict_page = false;    // is the page currently being read the dict page
    int32_t _cur_page_ordinal = 0;     // ordinal of the page currently being read (PME AAD)
    std::shared_ptr<::parquet::FileDecryptionProperties> _decryption_props;
    std::shared_ptr<::parquet::InternalFileDecryptor> _file_decryptor;
    std::shared_ptr<::parquet::Decryptor> _meta_decryptor; // page headers
    std::shared_ptr<::parquet::Decryptor> _data_decryptor; // page data
    std::vector<uint8_t> _decrypt_buf;                     // scratch for decrypted page data
    std::vector<uint8_t> _enc_header_buf;                  // scratch for the encrypted header module
    std::vector<uint8_t> _header_plain_buf;                // scratch for the decrypted header thrift
};

} // namespace starrocks::parquet
