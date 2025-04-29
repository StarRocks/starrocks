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

#include "formats/parquet/column_chunk_reader.h"

#include <glog/logging.h>

#include <memory>
#include <string>
#include <string_view>

#include "common/compiler_util.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "util/compression/block_compression.h"

namespace starrocks::parquet {

ColumnChunkReader::ColumnChunkReader(level_t max_def_level, level_t max_rep_level, int32_t type_length,
                                     const tparquet::ColumnChunk* column_chunk, const ColumnReaderOptions& opts)
        : _max_def_level(max_def_level),
          _max_rep_level(max_rep_level),
          _type_length(type_length),
          _chunk_metadata(column_chunk),
          _opts(opts),
          _def_level_decoder(&opts.stats->level_decode_ns),
          _rep_level_decoder(&opts.stats->level_decode_ns) {
    if (_chunk_metadata->meta_data.__isset.statistics && _chunk_metadata->meta_data.statistics.__isset.null_count &&
        _chunk_metadata->meta_data.statistics.null_count == 0) {
        _current_row_group_no_null = true;
    }
}

ColumnChunkReader::~ColumnChunkReader() = default;

Status ColumnChunkReader::init(int chunk_size) {
    int64_t start_offset = 0;
    if (metadata().__isset.dictionary_page_offset) {
        start_offset = metadata().dictionary_page_offset;
    } else {
        start_offset = metadata().data_page_offset;
    }
    int64_t size = metadata().total_compressed_size;
    int64_t num_values = metadata().num_values;
    _stream = _opts.file->stream().get();
    _page_reader = std::make_unique<PageReader>(_stream, start_offset, size, num_values, _opts, metadata().codec);

    // seek to the first page
    RETURN_IF_ERROR(_page_reader->seek_to_offset(start_offset));

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

Status ColumnChunkReader::next_page() {
    if (_page_parse_state != PAGE_DATA_PARSED && _page_parse_state == PAGE_HEADER_PARSED) {
        _opts.stats->page_skip += 1;
    }
    _page_parse_state = PAGE_DATA_PARSED;
    return _page_reader->next_page();
}

Status ColumnChunkReader::_parse_page_header() {
    SCOPED_RAW_TIMER(&_opts.stats->page_read_ns);
    DCHECK(_page_parse_state == INITIALIZED || _page_parse_state == PAGE_DATA_PARSED);
    RETURN_IF_ERROR(_page_reader->next_header());
    _page_parse_state = PAGE_HEADER_PARSED;

    // The page num values will be used for late materialization before parsing page data,
    // so we set _num_values when parsing header.
    auto& page_type = _page_reader->current_header()->type;
    // TODO: support DATA_PAGE_V2, now common writer use DATA_PAGE as default
    if (UNLIKELY(page_type != tparquet::PageType::DICTIONARY_PAGE && page_type != tparquet::PageType::DATA_PAGE &&
                 page_type != tparquet::PageType::DATA_PAGE_V2)) {
        return Status::NotSupported(strings::Substitute("Not supported page type: $0", page_type));
    }
    if (page_type == tparquet::PageType::DATA_PAGE) {
        const auto& page_header = _page_reader->current_header()->data_page_header;
        _num_values = page_header.num_values;
        _opts.stats->has_page_statistics |=
                (page_header.__isset.statistics &&
                 (page_header.statistics.__isset.min_value || page_header.statistics.__isset.min));
        _current_page_no_null = (page_header.__isset.statistics && page_header.statistics.__isset.null_count &&
                                 page_header.statistics.null_count == 0)
                                        ? true
                                        : false;
    } else if (page_type == tparquet::PageType::DATA_PAGE_V2) {
        const auto& page_header = _page_reader->current_header()->data_page_header_v2;
        _num_values = page_header.num_values;
        _opts.stats->has_page_statistics |=
                (page_header.__isset.statistics &&
                 (page_header.statistics.__isset.min_value || page_header.statistics.__isset.min));
        _current_page_no_null = (page_header.num_nulls == 0) ? true : false;
    }

    return Status::OK();
}

Status ColumnChunkReader::_parse_page_data() {
    SCOPED_RAW_TIMER(&_opts.stats->page_read_ns);
    switch (_page_reader->current_header()->type) {
    case tparquet::PageType::DATA_PAGE:
    case tparquet::PageType::DATA_PAGE_V2:
        RETURN_IF_ERROR(_parse_data_page(_page_reader->current_header()->type));
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

Status ColumnChunkReader::_parse_data_page(tparquet::PageType::type page_type) {
    if (_page_parse_state == PAGE_DATA_PARSED) {
        return Status::OK();
    }
    if (_page_parse_state != PAGE_HEADER_PARSED) {
        return Status::InternalError("Error state");
    }

    const auto& header = *_page_reader->current_header();
    ASSIGN_OR_RETURN(auto data, _page_reader->read_and_decompress_page_data());

    tparquet::Encoding::type encoding = tparquet::Encoding::PLAIN;

    if (page_type == tparquet::PageType::DATA_PAGE) {
        const auto& page_header = header.data_page_header;
        encoding = page_header.encoding;
        // parse levels
        if (_max_rep_level > 0) {
            RETURN_IF_ERROR(_rep_level_decoder.parse(page_header.repetition_level_encoding, _max_rep_level,
                                                     page_header.num_values, &data));
        }
        if (_max_def_level > 0) {
            RETURN_IF_ERROR(_def_level_decoder.parse(page_header.definition_level_encoding, _max_def_level,
                                                     page_header.num_values, &data));
        }
    } else if (page_type == tparquet::PageType::DATA_PAGE_V2) {
        const auto& page_header = header.data_page_header_v2;
        encoding = page_header.encoding;
        // parse levels
        if (_max_rep_level > 0) {
            RETURN_IF_ERROR(_rep_level_decoder.parse_v2(page_header.repetition_levels_byte_length, _max_rep_level,
                                                        page_header.num_values, &data));
        }
        if (_max_def_level > 0) {
            RETURN_IF_ERROR(_def_level_decoder.parse_v2(page_header.definition_levels_byte_length, _max_def_level,
                                                        page_header.num_values, &data));
        }
    }

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

    _cur_decoder->set_type_length(_type_length);
    RETURN_IF_ERROR(_cur_decoder->set_data(data));

    _page_parse_state = PAGE_DATA_PARSED;
    return Status::OK();
}

Status ColumnChunkReader::_parse_dict_page() {
    if (_dict_page_parsed) {
        return Status::InternalError("There are two dictionary page in this column");
    }

    const tparquet::PageHeader& header = *_page_reader->current_header();
    DCHECK_EQ(tparquet::PageType::DICTIONARY_PAGE, header.type);

    ASSIGN_OR_RETURN(auto data, _page_reader->read_and_decompress_page_data());

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
    RETURN_IF_ERROR(dict_decoder->set_data(data));
    dict_decoder->set_type_length(_type_length);

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

Status ColumnChunkReader::load_dictionary_page() {
    if (_dict_page_parsed) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_parse_page_header());
    if (UNLIKELY(!current_page_is_dict())) {
        return Status::InternalError("Not a dictionary page in dictionary page offset");
    }
    return _parse_dict_page();
}

bool ColumnChunkReader::current_page_is_dict() {
    const auto header = _page_reader->current_header();
    return header->type == tparquet::PageType::DICTIONARY_PAGE;
}
} // namespace starrocks::parquet
