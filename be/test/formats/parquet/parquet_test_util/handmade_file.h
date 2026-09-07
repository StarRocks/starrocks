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

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "base/bit/bit_util.h"
#include "base/bit/rle_encoding.h"
#include "base/coding.h"
#include "base/string/faststring.h"
#include "common/util/thrift_util.h"
#include "formats/parquet/types.h"
#include "gen_cpp/parquet_types.h"

namespace starrocks::parquet {

// Builds tiny Parquet files byte by byte, so that a test can state metadata that no sane
// writer would emit. Real writers keep `null_count` consistent with the definition levels;
// old parquet-mr does not always, and that is the case we need to be able to read.
//
// The produced file has exactly one OPTIONAL BYTE_ARRAY column named "c0", one row group and
// one uncompressed DATA_PAGE (v1) with RLE definition levels and PLAIN values.
class HandmadeParquetFile {
public:
    // `values` is the logical column: std::nullopt is a NULL, so the physical value area
    // holds only the engaged entries.
    struct Options {
        // Row-group level `ColumnMetaData.statistics.null_count`.
        std::optional<int64_t> row_group_null_count;
        // Page level `DataPageHeader.statistics.null_count`.
        std::optional<int64_t> page_null_count;
        // Row-group level min/max, already encoded the way PLAIN would write the value. Zone-map
        // pruning only kicks in when they are present, so a file that carries only `null_count`
        // cannot exercise that path at all.
        std::optional<std::string> row_group_min;
        std::optional<std::string> row_group_max;
        std::string created_by = "parquet-mr version 1.9.0-cdh6.3.2 (build handmade)";
    };

    // An OPTIONAL BYTE_ARRAY column.
    static std::string build(const std::vector<std::optional<std::string>>& values, const Options& options) {
        std::vector<std::optional<std::string>> encoded;
        encoded.reserve(values.size());
        for (const auto& value : values) {
            encoded.emplace_back(value.has_value() ? std::optional<std::string>(_plain_byte_array(*value))
                                                   : std::nullopt);
        }
        return _build(encoded, tparquet::Type::BYTE_ARRAY, options);
    }

    // An OPTIONAL INT32 column. Old parquet-mr min/max survive for non-BYTE_ARRAY types, so this
    // is the shape where a wrong `null_count` can still reach zone-map pruning.
    static std::string build_int32(const std::vector<std::optional<int32_t>>& values, const Options& options) {
        std::vector<std::optional<std::string>> encoded;
        encoded.reserve(values.size());
        for (const auto& value : values) {
            encoded.emplace_back(value.has_value() ? std::optional<std::string>(plain_int32(*value)) : std::nullopt);
        }
        return _build(encoded, tparquet::Type::INT32, options);
    }

    // PLAIN encoding of one INT32, handy for building `row_group_min` / `row_group_max`.
    static std::string plain_int32(int32_t value) {
        uint8_t buf[4];
        encode_fixed32_le(buf, static_cast<uint32_t>(value));
        return {reinterpret_cast<char*>(buf), sizeof(buf)};
    }

private:
    static std::string _plain_byte_array(const std::string& value) {
        uint8_t len[4];
        encode_fixed32_le(len, static_cast<uint32_t>(value.size()));
        return std::string(reinterpret_cast<char*>(len), sizeof(len)) + value;
    }

    // `values` holds each row's already-PLAIN-encoded bytes, or nullopt for a NULL row.
    static std::string _build(const std::vector<std::optional<std::string>>& values, tparquet::Type::type physical_type,
                              const Options& options) {
        const auto num_rows = static_cast<int64_t>(values.size());

        std::string page_body = _encode_page_body(values);
        std::string page_header = _serialize_page_header(page_body, num_rows, options);

        std::string file = "PAR1";
        const auto data_page_offset = static_cast<int64_t>(file.size());
        file += page_header;
        file += page_body;

        const auto chunk_size = static_cast<int64_t>(page_header.size() + page_body.size());
        std::string footer = _serialize_footer(num_rows, data_page_offset, chunk_size, physical_type, options);
        file += footer;

        uint8_t footer_len[4];
        encode_fixed32_le(footer_len, static_cast<uint32_t>(footer.size()));
        file.append(reinterpret_cast<char*>(footer_len), sizeof(footer_len));
        file += "PAR1";
        return file;
    }

    // definition levels (4-byte length prefix + RLE) followed by PLAIN-encoded values.
    static std::string _encode_page_body(const std::vector<std::optional<std::string>>& values) {
        // max_def_level is 1 for a flat OPTIONAL column.
        faststring rle_buffer;
        RleEncoder<level_t> encoder(&rle_buffer, BitUtil::log2(2));
        for (const auto& value : values) {
            encoder.Put(value.has_value() ? 1 : 0);
        }
        encoder.Flush();

        std::string body;
        uint8_t level_len[4];
        encode_fixed32_le(level_len, static_cast<uint32_t>(rle_buffer.size()));
        body.append(reinterpret_cast<char*>(level_len), sizeof(level_len));
        body.append(reinterpret_cast<const char*>(rle_buffer.data()), rle_buffer.size());

        for (const auto& value : values) {
            if (value.has_value()) {
                body += *value;
            }
        }
        return body;
    }

    static std::string _serialize_page_header(const std::string& page_body, int64_t num_rows, const Options& options) {
        tparquet::PageHeader header;
        header.__set_type(tparquet::PageType::DATA_PAGE);
        header.__set_uncompressed_page_size(static_cast<int32_t>(page_body.size()));
        header.__set_compressed_page_size(static_cast<int32_t>(page_body.size()));

        tparquet::DataPageHeader data_page_header;
        data_page_header.__set_num_values(static_cast<int32_t>(num_rows));
        data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
        data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
        if (options.page_null_count.has_value()) {
            tparquet::Statistics statistics;
            statistics.__set_null_count(*options.page_null_count);
            data_page_header.__set_statistics(statistics);
        }
        header.__set_data_page_header(data_page_header);
        return _serialize(&header);
    }

    static std::string _serialize_footer(int64_t num_rows, int64_t data_page_offset, int64_t chunk_size,
                                         tparquet::Type::type physical_type, const Options& options) {
        tparquet::SchemaElement root;
        root.__set_name("schema");
        root.__set_num_children(1);

        tparquet::SchemaElement column;
        column.__set_name("c0");
        column.__set_type(physical_type);
        column.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        if (physical_type == tparquet::Type::BYTE_ARRAY) {
            column.__set_converted_type(tparquet::ConvertedType::UTF8);
        }

        tparquet::ColumnMetaData meta_data;
        meta_data.__set_type(physical_type);
        meta_data.__set_encodings({tparquet::Encoding::PLAIN, tparquet::Encoding::RLE});
        meta_data.__set_path_in_schema({"c0"});
        meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        meta_data.__set_num_values(num_rows);
        meta_data.__set_total_uncompressed_size(chunk_size);
        meta_data.__set_total_compressed_size(chunk_size);
        meta_data.__set_data_page_offset(data_page_offset);
        if (options.row_group_null_count.has_value() || options.row_group_min.has_value()) {
            tparquet::Statistics statistics;
            if (options.row_group_null_count.has_value()) {
                statistics.__set_null_count(*options.row_group_null_count);
            }
            if (options.row_group_min.has_value()) {
                statistics.__set_min_value(*options.row_group_min);
                statistics.__set_min(*options.row_group_min);
            }
            if (options.row_group_max.has_value()) {
                statistics.__set_max_value(*options.row_group_max);
                statistics.__set_max(*options.row_group_max);
            }
            meta_data.__set_statistics(statistics);
        }

        tparquet::ColumnChunk chunk;
        chunk.__set_file_offset(data_page_offset);
        chunk.__set_meta_data(meta_data);

        tparquet::RowGroup row_group;
        row_group.__set_columns({chunk});
        row_group.__set_total_byte_size(chunk_size);
        row_group.__set_num_rows(num_rows);

        tparquet::FileMetaData file_meta_data;
        file_meta_data.__set_version(1);
        file_meta_data.__set_schema({root, column});
        file_meta_data.__set_num_rows(num_rows);
        file_meta_data.__set_row_groups({row_group});
        file_meta_data.__set_created_by(options.created_by);
        return _serialize(&file_meta_data);
    }

    template <class T>
    static std::string _serialize(T* obj) {
        ThriftSerializer serializer(true, 1024);
        std::string out;
        CHECK(serializer.serialize(obj, &out).ok());
        return out;
    }
};

} // namespace starrocks::parquet
