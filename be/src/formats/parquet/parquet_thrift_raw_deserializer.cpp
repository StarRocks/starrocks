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

#include "formats/parquet/parquet_thrift_raw_deserializer.h"

#include <thrift/transport/TBufferTransports.h>

#include <exception>
#include <memory>
#include <sstream>

// TCompactProtocol requires some #defines to work right. They also define
// UNLIKELY so we need to undef this.
#ifdef UNLIKELY
#undef UNLIKELY
#endif
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <thrift/protocol/TCompactProtocol.h>

namespace starrocks::parquet {
namespace {

std::shared_ptr<apache::thrift::TConfiguration> create_parquet_thrift_configuration(const ParquetThriftLimits& limits) {
    return std::make_shared<apache::thrift::TConfiguration>(limits.max_message_size, limits.max_frame_size,
                                                            limits.max_recursion_depth);
}

// Keep protocol construction and generated parquet thrift reads in StarRocksGen.
template <class T>
bool deserialize_parquet_compact_raw(const uint8_t* buf, uint32_t* len, const ParquetThriftLimits& limits,
                                     T* deserialized_msg, std::string* err) {
    auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>(
            const_cast<uint8_t*>(buf), *len, apache::thrift::transport::TMemoryBuffer::MemoryPolicy::OBSERVE,
            create_parquet_thrift_configuration(limits));
    apache::thrift::protocol::TCompactProtocolT<apache::thrift::transport::TMemoryBuffer> protocol(transport);

    try {
        deserialized_msg->read(&protocol);
    } catch (std::exception& e) {
        if (err != nullptr) {
            std::stringstream msg;
            msg << "couldn't deserialize thrift msg:\n" << e.what();
            *err = msg.str();
        }
        return false;
    } catch (...) {
        if (err != nullptr) {
            *err = "Unknown exception";
        }
        return false;
    }

    if (err != nullptr) {
        err->clear();
    }
    const uint32_t bytes_left = transport->available_read();
    *len -= bytes_left;
    return true;
}

} // namespace

bool deserialize_parquet_file_metadata_raw(const uint8_t* buf, uint32_t* len, const ParquetThriftLimits& limits,
                                           tparquet::FileMetaData* metadata, std::string* err) {
    return deserialize_parquet_compact_raw(buf, len, limits, metadata, err);
}

bool deserialize_parquet_page_header_raw(const uint8_t* buf, uint32_t* len, const ParquetThriftLimits& limits,
                                         tparquet::PageHeader* header, std::string* err) {
    return deserialize_parquet_compact_raw(buf, len, limits, header, err);
}

bool deserialize_parquet_column_index_raw(const uint8_t* buf, uint32_t* len, const ParquetThriftLimits& limits,
                                          tparquet::ColumnIndex* column_index, std::string* err) {
    return deserialize_parquet_compact_raw(buf, len, limits, column_index, err);
}

bool deserialize_parquet_offset_index_raw(const uint8_t* buf, uint32_t* len, const ParquetThriftLimits& limits,
                                          tparquet::OffsetIndex* offset_index, std::string* err) {
    return deserialize_parquet_compact_raw(buf, len, limits, offset_index, err);
}

bool deserialize_parquet_bloom_filter_header_raw(const uint8_t* buf, uint32_t* len, const ParquetThriftLimits& limits,
                                                 tparquet::BloomFilterHeader* header, std::string* err) {
    return deserialize_parquet_compact_raw(buf, len, limits, header, err);
}

} // namespace starrocks::parquet
