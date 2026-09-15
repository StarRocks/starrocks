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

#include "formats/parquet/page_reader.h"

#include <arrow/memory_pool.h>
#include <arrow/util/secure_string.h>
#include <glog/logging.h>
#include <parquet/encryption/encryption.h>
#include <parquet/encryption/encryption_internal.h>
#include <parquet/encryption/internal_file_decryptor.h>

#include <memory>
#include <ostream>
#include <span>
#include <vector>

#include "base/coding.h"
#include "base/compression/block_compression.h"
#include "base/container/raw_container.h"
#include "cache/datacache.h"
#include "common/compiler_util.h"
#include "common/config_scan_io_fwd.h"
#include "common/status.h"
#include "common/util/thrift_util.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/utils.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"

namespace starrocks::parquet {

// Reference for:
// https://github.com/apache/arrow/blob/7ebc88c8fae62ed97bc30865c845c8061132af7e/cpp/src/parquet/column_reader.h#L54-L57
static constexpr size_t kDefaultPageHeaderSize = 16 * 1024;
// 16MB is borrowed from Arrow
static constexpr size_t kMaxPageHeaderSize = 16 * 1024 * 1024;

PageReader::PageReader(io::SeekableInputStream* stream, size_t start_offset, size_t length, size_t num_values,
                       const ColumnReaderOptions& opts, const tparquet::CompressionCodec::type codec,
                       int16_t column_ordinal)
        : _stream(stream),
          _finish_offset(start_offset + length),
          _num_values_total(num_values),
          _opts(opts),
          _codec(codec),
          _column_ordinal(column_ordinal) {
    if (_opts.use_file_pagecache) {
        _cache = DataCache::GetInstance()->page_cache();
        _init_page_cache_key();
    }
    _compressed_buf = std::make_unique<std::vector<uint8_t>>();
    _uncompressed_buf = std::make_unique<std::vector<uint8_t>>();
    if (_opts.file_meta_data != nullptr && _opts.file_meta_data->is_encrypted()) {
        // Mark encrypted up front so a failed init surfaces as an error in next_header
        // rather than silently falling through to the plaintext page-header path.
        _encrypted = true;
        _encryption_status = _init_decryption();
    }
}

PageReader::~PageReader() = default;

Status PageReader::_init_decryption() {
    const auto& ctx = *_opts.file_meta_data->encryption_ctx();
    // The DEK comes from the scan range, never from the cached footer. Refusing here rather
    // than relying on the footer-parse path means an encrypted file with no planner-supplied key
    // fails on EVERY scan; when the key was cached alongside the footer, a cache hit skipped
    // that check entirely and decrypted the file anyway.
    if (_opts.parquet_encryption_info == nullptr || !_opts.parquet_encryption_info->__isset.file_dek ||
        _opts.parquet_encryption_info->file_dek.empty()) {
        return Status::NotSupported(
                "cannot read encrypted Parquet file: no decryption key was provided by the planner");
    }
    const std::string& dek = _opts.parquet_encryption_info->file_dek;
    try {
        const auto cipher = static_cast<::parquet::ParquetCipher::type>(ctx.algorithm);
        _decryption_props = ::parquet::FileDecryptionProperties::Builder()
                                    .footer_key(::arrow::util::SecureString(std::string(dek)))
                                    ->build();
        const std::string file_aad = ctx.aad_prefix + ctx.aad_file_unique;
        _file_decryptor = std::make_shared<::parquet::InternalFileDecryptor>(
                _decryption_props, file_aad, cipher, ctx.key_metadata, ::arrow::default_memory_pool());
        // Iceberg PME: encrypted footer, every column keyed with the footer key (no
        // per-column keys). arrow 24 dropped GetFooterDecryptorForColumn{Meta,Data}, and the
        // public GetColumn{Meta,Data}Decryptor cannot stand in: GetColumnKey() returns an
        // empty key when there is neither an explicit column key nor a key retriever, which
        // is exactly our case. Build the two Decryptors directly, the same way the (private)
        // GetFooterDecryptor(aad, metadata) does. metadata=true forces GCM, which is correct
        // for page headers even in a GCM_CTR file; metadata=false follows the file algorithm
        // for page data. The aad passed here is a placeholder -- both decryptors get
        // UpdateAad() per page before every Decrypt().
        const auto& footer_key = _file_decryptor->GetFooterKey();
        const auto key_len = static_cast<int32_t>(footer_key.size());
        _meta_decryptor = std::make_shared<::parquet::Decryptor>(
                ::parquet::encryption::AesDecryptor::Make(cipher, key_len, /*metadata=*/true), footer_key, file_aad,
                /*aad=*/"", ::arrow::default_memory_pool());
        _data_decryptor = std::make_shared<::parquet::Decryptor>(
                ::parquet::encryption::AesDecryptor::Make(cipher, key_len, /*metadata=*/false), footer_key, file_aad,
                /*aad=*/"", ::arrow::default_memory_pool());
    } catch (const ::parquet::ParquetException& e) {
        return Status::InternalError(fmt::format("failed to initialize Parquet page decryptor: {}", e.what()));
    }
    return Status::OK();
}

Status PageReader::_read_and_decrypt_header() {
    // The dictionary page (when present) is always the first page in the chunk; its
    // module type must be chosen by position because the page type is only known
    // after decryption.
    const bool is_dict = _has_dictionary_page && !_dict_page_read;
    int8_t header_module;
    if (is_dict) {
        _cur_is_dict_page = true;
        _cur_page_ordinal = ::parquet::kNonPageOrdinal;
        header_module = ::parquet::encryption::kDictionaryPageHeader;
    } else {
        _cur_is_dict_page = false;
        // The writer bakes the ABSOLUTE data-page index into the AAD, so this must be the page's real
        // index, not a count of headers this reader happened to read. _next_read_page_idx is exactly
        // that: it advances only for DATA_PAGE / DATA_PAGE_V2 (never the dictionary page, matching
        // parquet's "the page ordinal does not count the dictionary page"), and the page-skipping
        // reader sets it through set_next_read_page_idx() before seeking. Using a private sequential
        // counter here instead meant that seeking straight to page N built the AAD for a lower
        // ordinal and failed GCM verification on a perfectly valid file.
        _cur_page_ordinal = static_cast<int32_t>(_next_read_page_idx);
        header_module = ::parquet::encryption::kDataPageHeader;
    }

    // Read the length-prefixed encrypted header module: [4-byte len][nonce][ct][tag].
    uint8_t len_buf[4];
    RETURN_IF_ERROR(_stream->read_at_fully(_offset, len_buf, 4));
    const uint32_t module_len = decode_fixed32_le(len_buf);
    // Widened deliberately: the prefix spans the whole uint32 range, so `4 + module_len` in 32 bits
    // wraps for the top four values and a length of 0xFFFFFFFF would come out as 3 and sail past every
    // bound below.
    const uint64_t total_len = 4ULL + module_len;
    // Bound the length BEFORE allocating. This prefix sits outside the AEAD, so it is unauthenticated
    // attacker-controlled input: a corrupt or hostile file can ask for gigabytes and the allocation
    // would happen before read_at_fully() ever discovers the header runs past the chunk. Same two
    // bounds the plaintext header path applies -- the bytes actually left in this column chunk, and
    // kMaxPageHeaderSize.
    const uint64_t remaining = (_finish_offset > _offset) ? (_finish_offset - _offset) : 0;
    if (UNLIKELY(module_len == 0 || total_len > remaining || total_len > kMaxPageHeaderSize)) {
        return Status::Corruption(
                fmt::format("invalid encrypted Parquet page-header length: {} (chunk bytes remaining={}, max={}, "
                            "offset={}, finish={})",
                            module_len, remaining, kMaxPageHeaderSize, _offset, _finish_offset));
    }
    TRY_CATCH_BAD_ALLOC(raw::stl_vector_resize_uninitialized(&_enc_header_buf, total_len));
    RETURN_IF_ERROR(_stream->read_at_fully(_offset, _enc_header_buf.data(), total_len));

    try {
        const std::string aad =
                ::parquet::encryption::CreateModuleAad(_meta_decryptor->file_aad(), header_module,
                                                       _opts.row_group_ordinal, _column_ordinal, _cur_page_ordinal);
        _meta_decryptor->UpdateAad(aad);
        // arrow 24: Decrypt takes spans and the output is sized via PlaintextLength()
        // (CiphertextSizeDelta was removed). contains_length=true, so the 4-byte length
        // prefix stays part of the ciphertext span.
        const int32_t plain_cap = _meta_decryptor->PlaintextLength(static_cast<int32_t>(total_len));
        TRY_CATCH_BAD_ALLOC(raw::stl_vector_resize_uninitialized(&_header_plain_buf, plain_cap));
        const int plain_len = _meta_decryptor->Decrypt(
                std::span<const uint8_t>(_enc_header_buf.data(), static_cast<size_t>(total_len)),
                std::span<uint8_t>(_header_plain_buf.data(), static_cast<size_t>(plain_cap)));
        auto deser_len = static_cast<uint32_t>(plain_len);
        RETURN_IF_ERROR(
                deserialize_thrift_msg(_header_plain_buf.data(), &deser_len, TProtocolType::COMPACT, &_cur_header));
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption(fmt::format("failed to decrypt Parquet page header: {}", e.what()));
    }

    _header_length = static_cast<uint32_t>(total_len);
    _next_header_pos = _offset + _header_length + _data_length();
    RETURN_IF_ERROR(_skip_bytes(_header_length));
    _opts.stats->request_bytes_read += _header_length;
    _opts.stats->request_bytes_read_uncompressed += _header_length;
    if (is_dict) {
        _dict_page_read = true;
    }
    return Status::OK();
}

StatusOr<Slice> PageReader::_decrypt_page_module(const uint8_t* ciphertext, size_t ciphertext_len) {
    const int8_t data_module =
            _cur_is_dict_page ? ::parquet::encryption::kDictionaryPage : ::parquet::encryption::kDataPage;
    try {
        const std::string aad = ::parquet::encryption::CreateModuleAad(
                _data_decryptor->file_aad(), data_module, _opts.row_group_ordinal, _column_ordinal, _cur_page_ordinal);
        _data_decryptor->UpdateAad(aad);
        const int32_t plain_cap = _data_decryptor->PlaintextLength(static_cast<int32_t>(ciphertext_len));
        TRY_CATCH_BAD_ALLOC(raw::stl_vector_resize_uninitialized(&_decrypt_buf, plain_cap));
        const int plain_len =
                _data_decryptor->Decrypt(std::span<const uint8_t>(ciphertext, ciphertext_len),
                                         std::span<uint8_t>(_decrypt_buf.data(), static_cast<size_t>(plain_cap)));
        return Slice(_decrypt_buf.data(), plain_len);
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption(fmt::format("failed to decrypt Parquet page data: {}", e.what()));
    }
}

Status PageReader::next_page() {
    if (_opts.use_file_pagecache) {
        _cache_buf = nullptr;
        _hit_cache = false;
        _skip_page_cache = false;
    }
    return seek_to_offset(_next_header_pos);
}

Status PageReader::_deal_page_with_cache() {
    std::string& page_cache_key = _current_page_cache_key();
    PageCacheHandle cache_handle;
    bool ret = _cache->lookup(page_cache_key, &cache_handle);
    if (ret) {
        _hit_cache = true;
        _opts.stats->page_cache_read_counter += 1;
        // TODO: This is an ugly implementation. The _cache_buf is used both as a const pointer
        //  retrieved from the cache and as a temporary mutable pointer before insertion into the cache.
        //  Therefore, I must use const_cast here, which will be optimized later.
        _cache_buf =
                const_cast<std::vector<uint8_t>*>(reinterpret_cast<const std::vector<uint8_t>*>(cache_handle.data()));
        _page_handle = PageHandle(std::move(cache_handle));
        _header_length = _cache_buf->size();
        auto st = deserialize_thrift_msg(_cache_buf->data(), &_header_length, TProtocolType::COMPACT, &_cur_header);
        DCHECK(st.ok());
        _next_header_pos = _offset + _header_length + _data_length();
        RETURN_IF_ERROR(_skip_bytes(_header_length + _data_length()));
    } else {
        auto cache_buf = std::make_unique<std::vector<uint8_t>>();
        _cache_buf = cache_buf.get();
        RETURN_IF_ERROR(_read_and_deserialize_header(true));
        if (config::enable_adjustment_page_cache_skip && !_cache_decompressed_data()) {
            _skip_page_cache = true;
            return Status::OK();
        }
        RETURN_IF_ERROR(_read_and_decompress_internal(true));
        MemCacheWriteOptions opts{.evict_probability = _opts.datacache_options->datacache_evict_probability};
        auto st = _cache->insert(page_cache_key, _cache_buf, opts, &cache_handle);
        if (st.ok()) {
            _page_handle = PageHandle(std::move(cache_handle));
            _opts.stats->page_cache_write_counter += 1;
        } else {
            _page_handle = PageHandle(_cache_buf);
        }
        cache_buf.release();
    }

    return Status::OK();
}

Status PageReader::_read_and_deserialize_header(bool need_fill_cache) {
    size_t allowed_page_size = kDefaultPageHeaderSize;
    size_t remaining = _finish_offset - _offset;
    _header_length = 0;

    RETURN_IF_ERROR(_stream->seek(_offset));
    BufferPtr tmp_page_buffer;
    std::vector<uint8_t>* page_buffer;
    if (need_fill_cache) {
        DCHECK(_cache_buf);
        page_buffer = _cache_buf;
    } else {
        tmp_page_buffer = std::make_unique<std::vector<uint8_t>>();
        page_buffer = tmp_page_buffer.get();
    }

    do {
        allowed_page_size = std::min(std::min(allowed_page_size, remaining), kMaxPageHeaderSize);
        const uint8_t* page_buf = page_buffer->data();

        // prefer peek data instead to read data.
        bool peek_mode = false;
        {
            auto st = _stream->peek(allowed_page_size);
            if (!need_fill_cache && st.ok() && st.value().size() == allowed_page_size) {
                page_buf = (const uint8_t*)st.value().data();
                peek_mode = true;
            } else {
                TRY_CATCH_BAD_ALLOC(raw::stl_vector_resize_uninitialized(page_buffer, allowed_page_size));
                RETURN_IF_ERROR(_stream->read_at_fully(_offset, page_buffer->data(), allowed_page_size));
                page_buf = page_buffer->data();
                auto st = _stream->peek(allowed_page_size);
                if (st.ok()) {
                    _opts.stats->bytes_read -= allowed_page_size;
                    peek_mode = true;
                }
            }
        }

        _header_length = allowed_page_size;
        auto st = deserialize_thrift_msg(page_buf, &_header_length, TProtocolType::COMPACT, &_cur_header);

        if (st.ok()) {
            DCHECK(_header_length > 0);
            page_buffer->resize(_header_length);
            _next_header_pos = _offset + _header_length + _data_length();
            RETURN_IF_ERROR(_skip_bytes(_header_length));
            if (peek_mode) {
                _opts.stats->bytes_read += _header_length;
            }
            _opts.stats->request_bytes_read += _header_length;
            _opts.stats->request_bytes_read_uncompressed += _header_length;
            break;
        }

        if (UNLIKELY((allowed_page_size >= kMaxPageHeaderSize) || (_offset + allowed_page_size) >= _finish_offset)) {
            // Notice, here (_offset + allowed_page_size) >= _finish_offset
            // is using '>=' just to prevent loop infinitely.
            return Status::Corruption(
                    strings::Substitute("Failed to decode parquet page header, page header's size is out of range.  "
                                        "allowed_page_size=$0, max_page_size=$1, offset=$2, finish_offset=$3",
                                        allowed_page_size, kMaxPageHeaderSize, _offset, _finish_offset));
        }

        allowed_page_size *= 2;
    } while (true);
    return Status::OK();
}

Status PageReader::next_header() {
    if (_offset != _next_header_pos) {
        return Status::InternalError(
                strings::Substitute("Try to parse parquet column header in wrong position, offset=$0 vs expect=$1",
                                    _offset, _next_header_pos));
    }

    DCHECK(_num_values_read <= _num_values_total);
    if (_num_values_read >= _num_values_total || _next_read_page_idx >= _page_num) {
        LOG_IF(WARNING, _num_values_read > _num_values_total)
                << "Read more values than expected, read=" << _num_values_read << ", expect=" << _num_values_total;
        return Status::EndOfFile("");
    }

    if (_encrypted) {
        RETURN_IF_ERROR(_encryption_status);
        RETURN_IF_ERROR(_read_and_decrypt_header());
    } else if (_opts.use_file_pagecache) {
        RETURN_IF_ERROR(_deal_page_with_cache());
    } else {
        RETURN_IF_ERROR(_read_and_deserialize_header(false));
    }

    if (_cur_header.type == tparquet::PageType::DATA_PAGE) {
        _num_values_read += _cur_header.data_page_header.num_values;
        _next_read_page_idx++;
    } else if (_cur_header.type == tparquet::PageType::DATA_PAGE_V2) {
        _num_values_read += _cur_header.data_page_header_v2.num_values;
        _next_read_page_idx++;
    }
    return Status::OK();
}

Status PageReader::_read_bytes(void* buffer, size_t size) {
    if (_offset + size > _next_header_pos) {
        return Status::InternalError("Size to read exceed page size");
    }
    RETURN_IF_ERROR(_stream->read_at_fully(_offset, buffer, size));
    _offset += size;
    return Status::OK();
}

Status PageReader::_skip_bytes(size_t size) {
    if (UNLIKELY(_offset + size > _next_header_pos)) {
        return Status::InternalError("Size to skip exceed page size");
    }
    _offset += size;
    RETURN_IF_ERROR(_stream->skip(size));
    return Status::OK();
}

StatusOr<std::string_view> PageReader::_peek(size_t size) {
    if (_offset + size > _next_header_pos) {
        return Status::InternalError("Size to read exceed page size");
    }
    RETURN_IF_ERROR(_stream->seek(_offset));
    ASSIGN_OR_RETURN(auto ret, _stream->peek(size));
    return ret;
}

void PageReader::_init_page_cache_key() {
    auto& filename = _opts.file->filename();
    std::string key =
            ParquetUtils::get_file_cache_key(CacheType::PAGE, filename, _opts.modification_time, _opts.file_size);
    _page_cache_key.resize(22);
    char* data = _page_cache_key.data();
    memcpy(data, key.data(), key.size());
}

std::string& PageReader::_current_page_cache_key() {
    memcpy(_page_cache_key.data() + 14, &_offset, sizeof(_offset));
    return _page_cache_key;
}

StatusOr<Slice> PageReader::read_and_decompress_page_data() {
    _opts.stats->page_read_counter += 1;
    // Encrypted files never populate _cache_buf: the header path bypasses the page cache
    // (see next_header -> _read_and_decrypt_header), and page decryption only happens on the
    // non-cache path (_read_and_decompress_internal). Force encrypted pages down that path so
    // they are (a) decrypted and (b) do not dereference the null _cache_buf below (SIGSEGV).
    if (!_opts.use_file_pagecache || _skip_page_cache || _encrypted) {
        RETURN_IF_ERROR(_read_and_decompress_internal(false));
        return _uncompressed_data;
    } else {
        if (_cache_decompressed_data()) {
            if (_hit_cache) {
                _opts.stats->page_cache_read_decompressed_counter += 1;
            }
            _uncompressed_data = Slice(_cache_buf->data() + _header_length, _cache_buf->size() - _header_length);
        } else {
            if (_hit_cache) {
                _opts.stats->page_cache_read_compressed_counter += 1;
            }
            Slice input = Slice(_cache_buf->data() + _header_length, _cache_buf->size() - _header_length);
            TRY_CATCH_BAD_ALLOC(
                    raw::stl_vector_resize_uninitialized(_uncompressed_buf.get(), _cur_header.uncompressed_page_size));
            _uncompressed_data = Slice(_uncompressed_buf->data(), _cur_header.uncompressed_page_size);
            RETURN_IF_ERROR(_decompress_page(input, &_uncompressed_data));
        }
        return _uncompressed_data;
    }
}

bool PageReader::_cache_decompressed_data() {
    return _codec == tparquet::CompressionCodec::UNCOMPRESSED ||
           _cur_header.uncompressed_page_size <=
                   config::parquet_page_cache_decompress_threshold * _cur_header.compressed_page_size;
}

Status PageReader::_decompress_page(starrocks::Slice& input, starrocks::Slice* output) {
    if (_compress_codec == nullptr) {
        auto compress_type = ParquetUtils::convert_compression_codec(_codec);
        RETURN_IF_ERROR(get_block_compression_codec(compress_type, &_compress_codec));
    }
    if (_cur_header.type == tparquet::PageType::DATA_PAGE_V2) {
        auto uncompressed_size = output->size;
        auto* mark_pointer = output->data;
        uint32_t bytes_level_size = _cur_header.data_page_header_v2.definition_levels_byte_length +
                                    _cur_header.data_page_header_v2.repetition_levels_byte_length;
        memcpy(output->data, input.data, bytes_level_size);
        input.remove_prefix(bytes_level_size);
        output->remove_prefix(bytes_level_size);
        RETURN_IF_ERROR(_compress_codec->decompress(input, output));
        *output = Slice(mark_pointer, uncompressed_size);
    } else {
        RETURN_IF_ERROR(_compress_codec->decompress(input, output));
    }
    return Status::OK();
}

Status PageReader::_read_and_decompress_internal(bool need_fill_cache) {
    bool is_compressed = true;
    if (_cur_header.type == tparquet::PageType::DATA_PAGE_V2) {
        const auto& page_header = _cur_header.data_page_header_v2;
        if (page_header.__isset.is_compressed) {
            is_compressed = page_header.is_compressed;
        }
    }

    // ARROW-17100: [C++][Parquet] Fix backwards compatibility for ParquetV2 data pages written prior to 3.0.0 per ARROW-10353 #13665
    // https://github.com/apache/arrow/pull/13665/files
    // Prior to Arrow 3.0.0, is_compressed was always set to false in column headers,
    // even if compression was used. See ARROW-17100.
    bool always_compressed = (_opts.file_meta_data->writer_version().IsAlwaysCompressed());
    is_compressed |= always_compressed;

    is_compressed = is_compressed && (_codec != tparquet::CompressionCodec::UNCOMPRESSED);

    RETURN_IF_ERROR(CurrentThread::mem_tracker()->check_mem_limit("read and decompress page"));

    size_t uncompressed_size = _cur_header.uncompressed_page_size;
    // based on parquet.thrift Line 571~575, for DATA_PAGE_V2, even when is_compressed is set as false,
    // data length is decided by compressed_page_size
    size_t read_size = _data_length();
    _opts.stats->request_bytes_read += read_size;
    _opts.stats->request_bytes_read_uncompressed += uncompressed_size;

    // check if we can zero copy read.
    Slice read_data;
    DCHECK(_next_header_pos - _offset == read_size);
    auto ret = _peek(read_size);
    if (!need_fill_cache && ret.ok() && ret.value().size() == read_size) {
        _opts.stats->bytes_read += read_size;
        // peek dos not advance offset.
        RETURN_IF_ERROR(_skip_bytes(read_size));
        read_data = Slice(ret.value().data(), read_size);
    } else {
        std::vector<uint8_t>& read_buffer = is_compressed ? *_compressed_buf : *_uncompressed_buf;
        if (!need_fill_cache || (is_compressed && _cache_decompressed_data())) {
            TRY_CATCH_BAD_ALLOC(read_buffer.reserve(read_size));
            read_data = Slice(read_buffer.data(), read_size);
        } else {
            auto original_size = _cache_buf->size();
            TRY_CATCH_BAD_ALLOC(raw::stl_vector_resize_uninitialized(_cache_buf, original_size + read_size));
            read_data = Slice(_cache_buf->data() + original_size, read_size);
        }
        RETURN_IF_ERROR(_read_bytes(read_data.data, read_data.size));
    }

    // For an encrypted file, read_data currently holds the encrypted page-data module
    // (its on-disk size == compressed_page_size). Decrypt it in place of the original
    // bytes; the plaintext (still-compressed) data then flows into decompression. This
    // sits below all of StarRocks' decode/filter logic, so those optimizations are
    // unaffected. (Encrypted files use the buffered, non-cache path: need_fill_cache
    // is always false here.)
    if (_encrypted) {
        ASSIGN_OR_RETURN(read_data,
                         _decrypt_page_module(reinterpret_cast<const uint8_t*>(read_data.data), read_data.size));
    }

    // if it's compressed, we have to uncompress page
    // otherwise we just assign slice.
    if (is_compressed) {
        if (!need_fill_cache) {
            TRY_CATCH_BAD_ALLOC(raw::stl_vector_resize_uninitialized(_uncompressed_buf.get(), uncompressed_size));
            _uncompressed_data = Slice(_uncompressed_buf->data(), uncompressed_size);
            return _decompress_page(read_data, &_uncompressed_data);
        } else if (_cache_decompressed_data()) {
            auto original_size = _cache_buf->size();
            TRY_CATCH_BAD_ALLOC(raw::stl_vector_resize_uninitialized(_cache_buf, uncompressed_size + original_size));
            _uncompressed_data = Slice(_cache_buf->data() + original_size, uncompressed_size);
            return _decompress_page(read_data, &_uncompressed_data);
        }
        // if we cache compressed data, we can decompress it later.
    } else {
        _uncompressed_data = read_data;
    }
    return Status::OK();
}

} // namespace starrocks::parquet
