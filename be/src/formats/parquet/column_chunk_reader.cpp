// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "formats/parquet/column_chunk_reader.h"

#include <memory>

#include "common/status.h"
#include "exec/vectorized/hdfs_scanner.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/page_reader.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "util/runtime_profile.h"

namespace starrocks::parquet {

ColumnChunkReader::ColumnChunkReader(level_t max_def_level, level_t max_rep_level, int32_t type_length,
                                     const tparquet::ColumnChunk* column_chunk, const ColumnReaderOptions& opts)
        : _max_def_level(max_def_level),
          _max_rep_level(max_rep_level),
          _type_length(type_length),
          _chunk_metadata(column_chunk),
          _opts(opts) {}

ColumnChunkReader::~ColumnChunkReader() = default;

Status ColumnChunkReader::init(int chunk_size) {
    int64_t start_offset = 0;
    if (metadata().__isset.dictionary_page_offset) {
        start_offset = metadata().dictionary_page_offset;
    } else {
        start_offset = metadata().data_page_offset;
    }
    size_t size = metadata().total_compressed_size;
    IBufferedInputStream* stream = _opts.sb_stream;
    if (stream == nullptr) {
        _default_stream = std::make_unique<DefaultBufferedInputStream>(_opts.file, start_offset, size);
        _default_stream->reserve(config::parquet_buffer_stream_reserve_size);
        stream = _default_stream.get();
    }
    _page_reader = std::make_unique<PageReader>(stream, start_offset, size);

    // seek to the first page
    _page_reader->seek_to_offset(start_offset);

    auto compress_type = convert_compression_codec(metadata().codec);
    RETURN_IF_ERROR(get_block_compression_codec(compress_type, &_compress_codec));

    _chunk_size = chunk_size;
    return Status::OK();
}

Status ColumnChunkReader::load_header() {
    RETURN_IF_ERROR(_parse_page_header());
    return Status::OK();
}

Status ColumnChunkReader::load_page() {
    if (_page_parse_state == PAGE_DATA_PARSED) {
        return Status::OK();
    }
    if (_page_parse_state != PAGE_HEADER_PARSED) {
        return Status::InternalError("Page header has not been parsed before loading page data");
    }
    return _parse_page_data();
}

Status ColumnChunkReader::skip_page() {
    if (_page_parse_state == PAGE_DATA_PARSED) {
        return Status::OK();
    }
    if (_page_parse_state != PAGE_HEADER_PARSED) {
        return Status::InternalError("Page header has not been parsed before skiping page data");
    }
    const auto& header = *_page_reader->current_header();
    uint32_t compressed_size = header.compressed_page_size;
    uint32_t uncompressed_size = header.uncompressed_page_size;
    size_t size = _compress_codec != nullptr ? compressed_size : uncompressed_size;
    RETURN_IF_ERROR(_page_reader->skip_bytes(size));
    _opts.stats->skip_read_rows += _num_values;

    _page_parse_state = PAGE_DATA_PARSED;
    return Status::OK();
}

Status ColumnChunkReader::_parse_page_header() {
    DCHECK(_page_parse_state == INITIALIZED || _page_parse_state == PAGE_DATA_PARSED);
    RETURN_IF_ERROR(_page_reader->next_header());

    // The page num values will be used for late materialization before parsing page data,
    // so we set _num_values when parsing header.
    if (_page_reader->current_header()->type == tparquet::PageType::DATA_PAGE) {
        const auto& header = *_page_reader->current_header();
        _num_values = header.data_page_header.num_values;
    }

    _page_parse_state = PAGE_HEADER_PARSED;
    return Status::OK();
}

Status ColumnChunkReader::_parse_page_data() {
    switch (_page_reader->current_header()->type) {
    case tparquet::PageType::DATA_PAGE:
        RETURN_IF_ERROR(_parse_data_page());
        break;
    case tparquet::PageType::DICTIONARY_PAGE:
        RETURN_IF_ERROR(_parse_dict_page());
        break;
    default:
        return Status::NotSupported(
                strings::Substitute("Not supported page type: $0", _page_reader->current_header()->type));
        break;
    }
    return Status::OK();
}

void ColumnChunkReader::_reserve_uncompress_buf(size_t size) {
    if (size <= _uncompressed_buf_capacity) {
        return;
    }
    auto new_capacity = BitUtil::next_power_of_two(size);
    _uncompressed_buf.reset(new uint8_t[new_capacity]);
    _uncompressed_buf_capacity = new_capacity;
}

Status ColumnChunkReader::_read_and_decompress_page_data(uint32_t compressed_size, uint32_t uncompressed_size,
                                                         bool is_compressed) {
    RETURN_IF_ERROR(CurrentThread::mem_tracker()->check_mem_limit("read and decompress page"));
    if (is_compressed && _compress_codec != nullptr) {
        Slice com_slice("", compressed_size);
        RETURN_IF_ERROR(_page_reader->read_bytes((const uint8_t**)&com_slice.data, com_slice.size));

        _reserve_uncompress_buf(uncompressed_size);
        _data = Slice(_uncompressed_buf.get(), uncompressed_size);
        RETURN_IF_ERROR(_compress_codec->decompress(com_slice, &_data));
    } else {
        _data.size = uncompressed_size;
        RETURN_IF_ERROR(_page_reader->read_bytes((const uint8_t**)&_data.data, _data.size));
    }

    return Status::OK();
}

Status ColumnChunkReader::_parse_data_page() {
    if (_page_parse_state == PAGE_DATA_PARSED) {
        return Status::OK();
    }
    if (_page_parse_state != PAGE_HEADER_PARSED) {
        return Status::InternalError("Error state");
    }

    const auto& header = *_page_reader->current_header();

    uint32_t compressed_size = header.compressed_page_size;
    uint32_t uncompressed_size = header.uncompressed_page_size;
    RETURN_IF_ERROR(_read_and_decompress_page_data(compressed_size, uncompressed_size, true));

    // parse levels
    if (_max_rep_level > 0) {
        RETURN_IF_ERROR(_rep_level_decoder.parse(header.data_page_header.repetition_level_encoding, _max_rep_level,
                                                 header.data_page_header.num_values, &_data));
    }
    if (_max_def_level > 0) {
        RETURN_IF_ERROR(_def_level_decoder.parse(header.data_page_header.definition_level_encoding, _max_def_level,
                                                 header.data_page_header.num_values, &_data));
    }

    auto encoding = header.data_page_header.encoding;
    // change the deprecated encoding to RLE_DICTIONARY
    if (encoding == tparquet::Encoding::PLAIN_DICTIONARY) {
        encoding = tparquet::Encoding::RLE_DICTIONARY;
    }

    _cur_decoder = _decoders[static_cast<int>(encoding)].get();
    if (_cur_decoder == nullptr) {
        std::unique_ptr<Decoder> decoder;
        const EncodingInfo* enc_info = nullptr;
        RETURN_IF_ERROR(EncodingInfo::get(metadata().type, encoding, &enc_info));
        RETURN_IF_ERROR(enc_info->create_decoder(&decoder));

        _cur_decoder = decoder.get();
        _decoders[static_cast<int>(encoding)] = std::move(decoder);
    }

    _cur_decoder->set_type_legth(_type_length);
    _cur_decoder->set_data(_data);

    _page_parse_state = PAGE_DATA_PARSED;
    return Status::OK();
}

Status ColumnChunkReader::_parse_dict_page() {
    if (_dict_page_parsed) {
        return Status::InternalError("There are two dictionary page in this column");
    }

    const tparquet::PageHeader& header = *_page_reader->current_header();
    DCHECK_EQ(tparquet::PageType::DICTIONARY_PAGE, header.type);

    uint32_t compressed_size = header.compressed_page_size;
    uint32_t uncompressed_size = header.uncompressed_page_size;
    RETURN_IF_ERROR(_read_and_decompress_page_data(compressed_size, uncompressed_size, true));

    // initialize dict decoder to decode dictionary
    std::unique_ptr<Decoder> dict_decoder;

    tparquet::Encoding::type dict_encoding = header.dictionary_page_header.encoding;
    // Using the PLAIN_DICTIONARY enum value is deprecated in the Parquet 2.0 specification.
    // Prefer using RLE_DICTIONARY in a data page and PLAIN in a dictionary page for Parquet 2.0+ files.
    // refer: https://github.com/apache/parquet-format/blob/master/Encodings.md
    if (dict_encoding == tparquet::Encoding::PLAIN_DICTIONARY) {
        dict_encoding = tparquet::Encoding::PLAIN;
    }

    const EncodingInfo* code_info = nullptr;
    RETURN_IF_ERROR(EncodingInfo::get(metadata().type, dict_encoding, &code_info));
    RETURN_IF_ERROR(code_info->create_decoder(&dict_decoder));
    dict_decoder->set_data(_data);
    dict_decoder->set_type_legth(_type_length);

    // initialize decoder
    std::unique_ptr<Decoder> decoder;
    RETURN_IF_ERROR(EncodingInfo::get(metadata().type, tparquet::Encoding::RLE_DICTIONARY, &code_info));
    RETURN_IF_ERROR(code_info->create_decoder(&decoder));
    RETURN_IF_ERROR(decoder->set_dict(_chunk_size, header.dictionary_page_header.num_values, dict_decoder.get()));

    int rle_encoding = static_cast<int>(tparquet::Encoding::RLE_DICTIONARY);
    _decoders[rle_encoding] = std::move(decoder);
    _cur_decoder = _decoders[rle_encoding].get();
    _dict_page_parsed = true;

    _page_parse_state = PAGE_DATA_PARSED;
    return Status::OK();
}

Status ColumnChunkReader::_try_load_dictionary() {
    if (_dict_page_parsed) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_parse_page_header());
    if (!current_page_is_dict()) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_parse_dict_page());
    return Status::OK();
}

bool ColumnChunkReader::current_page_is_dict() {
    const auto header = _page_reader->current_header();
    return header->type == tparquet::PageType::DICTIONARY_PAGE;
}

} // namespace starrocks::parquet
