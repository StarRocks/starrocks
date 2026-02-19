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

#include "formats/parquet/utils.h"

#include <glog/logging.h>
#include <parquet/metadata.h>

#include <cstring>

#include "formats/parquet/schema.h"
#include "util/hash_util.hpp"

namespace starrocks::parquet {

static TypeDescriptor _decimal_type_from_field(const ParquetField& field) {
    const int precision = field.precision > 0 ? field.precision : TypeDescriptor::MAX_DECIMAL8_PRECISION;
    const int scale = field.scale >= 0 ? field.scale : 0;
    if (precision <= TypeDescriptor::MAX_DECIMAL4_PRECISION) {
        return TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, precision, scale);
    }
    if (precision <= TypeDescriptor::MAX_DECIMAL8_PRECISION) {
        return TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, precision, scale);
    }
    return TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, precision, scale);
}

static LogicalType _logical_type_from_integer(const tparquet::IntType& int_type) {
    const int bit_width = int_type.bitWidth;
    if (!int_type.isSigned) {
        if (bit_width <= 8) {
            return TYPE_SMALLINT;
        }
        if (bit_width <= 16) {
            return TYPE_INT;
        }
        return TYPE_BIGINT;
    }
    if (bit_width <= 8) {
        return TYPE_TINYINT;
    }
    if (bit_width <= 16) {
        return TYPE_SMALLINT;
    }
    if (bit_width <= 32) {
        return TYPE_INT;
    }
    return TYPE_BIGINT;
}

static TypeDescriptor _primitive_type_from_field(const ParquetField& field) {
    const auto& schema_element = field.schema_element;

    if (schema_element.__isset.logicalType) {
        const auto& logical_type = schema_element.logicalType;
        if (logical_type.__isset.STRING) {
            return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        }
        if (logical_type.__isset.JSON) {
            return TypeDescriptor::create_json_type();
        }
        if (logical_type.__isset.BSON) {
            return TypeDescriptor::create_json_type();
        }
        if (logical_type.__isset.DATE) {
            return TypeDescriptor(TYPE_DATE);
        }
        if (logical_type.__isset.TIME) {
            return TypeDescriptor(TYPE_TIME);
        }
        if (logical_type.__isset.TIMESTAMP) {
            return TypeDescriptor(TYPE_DATETIME);
        }
        if (logical_type.__isset.INTEGER) {
            return TypeDescriptor(_logical_type_from_integer(logical_type.INTEGER));
        }
        if (logical_type.__isset.DECIMAL) {
            return _decimal_type_from_field(field);
        }
        if (logical_type.__isset.UUID) {
            return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        }
    } else if (schema_element.__isset.converted_type) {
        switch (schema_element.converted_type) {
        case tparquet::ConvertedType::UTF8:
            return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        case tparquet::ConvertedType::JSON:
            return TypeDescriptor::create_json_type();
        case tparquet::ConvertedType::DATE:
            return TypeDescriptor(TYPE_DATE);
        case tparquet::ConvertedType::TIME_MILLIS:
        case tparquet::ConvertedType::TIME_MICROS:
            return TypeDescriptor(TYPE_TIME);
        case tparquet::ConvertedType::TIMESTAMP_MILLIS:
        case tparquet::ConvertedType::TIMESTAMP_MICROS:
            return TypeDescriptor(TYPE_DATETIME);
        case tparquet::ConvertedType::DECIMAL:
            return _decimal_type_from_field(field);
        default:
            break;
        }
    }

    switch (field.physical_type) {
    case tparquet::Type::BOOLEAN:
        return TypeDescriptor(TYPE_BOOLEAN);
    case tparquet::Type::INT32:
        return TypeDescriptor(TYPE_INT);
    case tparquet::Type::INT64:
        return TypeDescriptor(TYPE_BIGINT);
    case tparquet::Type::INT96:
        return TypeDescriptor(TYPE_DATETIME);
    case tparquet::Type::FLOAT:
        return TypeDescriptor(TYPE_FLOAT);
    case tparquet::Type::DOUBLE:
        return TypeDescriptor(TYPE_DOUBLE);
    case tparquet::Type::BYTE_ARRAY:
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    default:
        return TypeDescriptor(TYPE_UNKNOWN);
    }
}

TypeDescriptor ParquetUtils::to_type_desc(const ParquetField& field) {
    if (field.type == ColumnType::STRUCT) {
        std::vector<std::string> field_names;
        std::vector<TypeDescriptor> children;
        field_names.reserve(field.children.size());
        children.reserve(field.children.size());
        for (const auto& child : field.children) {
            field_names.emplace_back(child.name);
            children.emplace_back(to_type_desc(child));
        }
        return TypeDescriptor::create_struct_type(field_names, children);
    }
    if (field.type == ColumnType::ARRAY) {
        if (field.children.empty()) {
            return TypeDescriptor::create_array_type(TypeDescriptor(TYPE_UNKNOWN));
        }
        return TypeDescriptor::create_array_type(to_type_desc(field.children[0]));
    }
    if (field.type == ColumnType::MAP) {
        if (field.children.size() < 2) {
            return TypeDescriptor::create_map_type(TypeDescriptor(TYPE_UNKNOWN), TypeDescriptor(TYPE_UNKNOWN));
        }
        return TypeDescriptor::create_map_type(to_type_desc(field.children[0]), to_type_desc(field.children[1]));
    }
    return _primitive_type_from_field(field);
}

CompressionTypePB ParquetUtils::convert_compression_codec(tparquet::CompressionCodec::type codec) {
    switch (codec) {
    case tparquet::CompressionCodec::UNCOMPRESSED:
        return NO_COMPRESSION;
    case tparquet::CompressionCodec::SNAPPY:
        return SNAPPY;
    // parquet-mr uses hadoop-lz4. more details refers to https://issues.apache.org/jira/browse/PARQUET-1878
    case tparquet::CompressionCodec::LZ4:
        return LZ4_HADOOP;
    case tparquet::CompressionCodec::ZSTD:
        return ZSTD;
    case tparquet::CompressionCodec::GZIP:
        return GZIP;
    case tparquet::CompressionCodec::LZO:
        return LZO;
    case tparquet::CompressionCodec::BROTLI:
        return BROTLI;
    case tparquet::CompressionCodec::LZ4_RAW:
        return LZ4;
    default:
        return UNKNOWN_COMPRESSION;
    }
}

int decimal_precision_to_byte_count_inner(int precision) {
    return std::ceil((std::log(std::pow(10, precision) - 1) / std::log(2) + 1) / 8);
}

int ParquetUtils::decimal_precision_to_byte_count(int precision) {
    DCHECK(precision > 0 && precision <= 38);
    static std::array<int, 39> table = {
            0,
            decimal_precision_to_byte_count_inner(1),
            decimal_precision_to_byte_count_inner(2),
            decimal_precision_to_byte_count_inner(3),
            decimal_precision_to_byte_count_inner(4),
            decimal_precision_to_byte_count_inner(5),
            decimal_precision_to_byte_count_inner(6),
            decimal_precision_to_byte_count_inner(7),
            decimal_precision_to_byte_count_inner(8),
            decimal_precision_to_byte_count_inner(9),
            decimal_precision_to_byte_count_inner(10),
            decimal_precision_to_byte_count_inner(11),
            decimal_precision_to_byte_count_inner(12),
            decimal_precision_to_byte_count_inner(13),
            decimal_precision_to_byte_count_inner(14),
            decimal_precision_to_byte_count_inner(15),
            decimal_precision_to_byte_count_inner(16),
            decimal_precision_to_byte_count_inner(17),
            decimal_precision_to_byte_count_inner(18),
            decimal_precision_to_byte_count_inner(19),
            decimal_precision_to_byte_count_inner(20),
            decimal_precision_to_byte_count_inner(21),
            decimal_precision_to_byte_count_inner(22),
            decimal_precision_to_byte_count_inner(23),
            decimal_precision_to_byte_count_inner(24),
            decimal_precision_to_byte_count_inner(25),
            decimal_precision_to_byte_count_inner(26),
            decimal_precision_to_byte_count_inner(27),
            decimal_precision_to_byte_count_inner(28),
            decimal_precision_to_byte_count_inner(29),
            decimal_precision_to_byte_count_inner(30),
            decimal_precision_to_byte_count_inner(31),
            decimal_precision_to_byte_count_inner(32),
            decimal_precision_to_byte_count_inner(33),
            decimal_precision_to_byte_count_inner(34),
            decimal_precision_to_byte_count_inner(35),
            decimal_precision_to_byte_count_inner(36),
            decimal_precision_to_byte_count_inner(37),
            decimal_precision_to_byte_count_inner(38),
    };

    return table[precision];
}

std::vector<int64_t> ParquetUtils::collect_split_offsets(const ::parquet::FileMetaData& meta_data) {
    std::vector<int64_t> split_offsets;
    split_offsets.reserve(meta_data.num_row_groups());
    for (int i = 0; i < meta_data.num_row_groups(); i++) {
        auto first_column_meta = meta_data.RowGroup(i)->ColumnChunk(0);
        int64_t dict_page_offset = first_column_meta->dictionary_page_offset();
        int64_t first_data_page_offset = first_column_meta->data_page_offset();
        int64_t split_offset = dict_page_offset > 0 && dict_page_offset < first_data_page_offset
                                       ? dict_page_offset
                                       : first_data_page_offset;
        split_offsets.emplace_back(split_offset);
    }
    return split_offsets;
}

int64_t ParquetUtils::get_column_start_offset(const tparquet::ColumnMetaData& column) {
    int64_t offset = column.data_page_offset;
    if (column.__isset.index_page_offset) {
        offset = std::min(offset, column.index_page_offset);
    }
    if (column.__isset.dictionary_page_offset) {
        offset = std::min(offset, column.dictionary_page_offset);
    }
    return offset;
}

int64_t ParquetUtils::get_row_group_start_offset(const tparquet::RowGroup& row_group) {
    const tparquet::ColumnMetaData& first_column = row_group.columns[0].meta_data;
    int64_t offset = get_column_start_offset(first_column);

    if (row_group.__isset.file_offset) {
        offset = std::min(offset, row_group.file_offset);
    }
    return offset;
}

int64_t ParquetUtils::get_row_group_end_offset(const tparquet::RowGroup& row_group) {
    // following computation is not correct. `total_compressed_size` means compressed size of all columns
    // but between columns there could be holes, which means end offset inaccurate.
    // if (row_group.__isset.file_offset && row_group.__isset.total_compressed_size) {
    //     return row_group.file_offset + row_group.total_compressed_size;
    // }
    const tparquet::ColumnMetaData& last_column = row_group.columns.back().meta_data;
    return get_column_start_offset(last_column) + last_column.total_compressed_size;
}

std::string ParquetUtils::get_file_cache_key(CacheType type, const std::string& filename, int64_t modification_time,
                                             uint64_t file_size) {
    std::string key;
    key.resize(14);
    char* data = key.data();
    uint64_t hash_value = HashUtil::hash64(filename.data(), filename.size(), 0);
    memcpy(data, &hash_value, sizeof(hash_value));
    const std::string& prefix = cache_key_prefix[type];
    memcpy(data + 8, prefix.data(), prefix.size());
    // The modification time is more appropriate to indicate the different file versions.
    // While some data source, such as Hudi, have no modification time because their files
    // cannot be overwritten. So, if the modification time is unsupported, we use file size instead.
    // Also, to reduce memory usage, we only use the high four bytes to represent the second timestamp.
    if (modification_time > 0) {
        uint32_t mtime_s = (modification_time >> 9) & 0x00000000FFFFFFFF;
        memcpy(data + 10, &mtime_s, sizeof(mtime_s));
    } else {
        uint32_t size = file_size;
        memcpy(data + 10, &size, sizeof(size));
    }
    return key;
}

} // namespace starrocks::parquet
