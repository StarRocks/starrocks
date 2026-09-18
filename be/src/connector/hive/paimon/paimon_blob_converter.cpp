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

#include "connector/hive/paimon/paimon_blob_converter.h"

#include <arrow/array/array_binary.h>
#include <arrow/type.h>
#include <fmt/format.h>

#include <cstring>
#include <optional>
#include <string_view>

#include "column/file_column.h"
#include "column/nullable_column.h"

namespace starrocks {

namespace {

constexpr int8_t kBlobDescriptorVersion = 2;
constexpr int64_t kBlobDescriptorMagic = 0x424C4F4244455343LL; // "BLOBDESC"
constexpr size_t kBlobDescriptorHeaderSize = sizeof(int8_t) + sizeof(int64_t) + sizeof(int32_t);
constexpr size_t kBlobDescriptorTrailerSize = sizeof(int64_t) + sizeof(int64_t);

template <typename T>
T read_le(const char* p) {
    T v;
    std::memcpy(&v, p, sizeof(T));
    return v; // StarRocks BE targets little-endian hosts only.
}

struct BlobDescriptor {
    std::string_view uri;
    int64_t offset = 0;
    int64_t length = -1; // -1: up to the end of the file
};

// Returns the decoded descriptor when `value` is a version-2 paimon BlobDescriptor.
std::optional<BlobDescriptor> parse_blob_descriptor(std::string_view value) {
    if (value.size() < kBlobDescriptorHeaderSize + kBlobDescriptorTrailerSize) {
        return std::nullopt;
    }
    const char* p = value.data();
    if (read_le<int8_t>(p) != kBlobDescriptorVersion || read_le<int64_t>(p + 1) != kBlobDescriptorMagic) {
        return std::nullopt;
    }
    const int32_t uri_len = read_le<int32_t>(p + 9);
    if (uri_len < 0 ||
        value.size() != kBlobDescriptorHeaderSize + static_cast<size_t>(uri_len) + kBlobDescriptorTrailerSize) {
        return std::nullopt;
    }
    BlobDescriptor desc;
    desc.uri = value.substr(kBlobDescriptorHeaderSize, uri_len);
    const char* trailer = p + kBlobDescriptorHeaderSize + uri_len;
    desc.offset = read_le<int64_t>(trailer);
    desc.length = read_le<int64_t>(trailer + sizeof(int64_t));
    if (desc.offset < 0 || desc.length < -1) {
        return std::nullopt;
    }
    return desc;
}

void append_value(FileColumn* file_column, std::string_view value) {
    if (auto desc = parse_blob_descriptor(value)) {
        std::optional<int64_t> size;
        if (desc->length >= 0) {
            size = desc->length;
        }
        file_column->append_reference(Slice(desc->uri.data(), desc->uri.size()), desc->offset, size);
        return;
    }
    file_column->append_inline(Slice(value.data(), value.size()));
}

template <typename ArrayType>
void append_binary_array(const ArrayType* array, size_t start, size_t num_rows, NullableColumn* dst) {
    auto* file_column = down_cast<FileColumn*>(dst->data_column_raw_ptr());
    NullColumn* null_column = dst->null_column_raw_ptr();
    bool has_null = false;
    for (size_t i = 0; i < num_rows; ++i) {
        const int64_t idx = static_cast<int64_t>(start + i);
        if (array->IsNull(idx)) {
            file_column->append_null_row();
            null_column->append(1);
            has_null = true;
            continue;
        }
        append_value(file_column, array->GetView(idx));
        null_column->append(0);
    }
    dst->set_has_null(has_null);
}

} // namespace

Status append_paimon_blob_to_file_column(const arrow::Array* array, size_t start, size_t num_rows, Column* dst) {
    if (array == nullptr) {
        return Status::InternalError("Paimon BLOB column is missing from the arrow batch");
    }
    if (!dst->is_nullable()) {
        return Status::InternalError("FILE column must be nullable");
    }
    auto* nullable = down_cast<NullableColumn*>(dst);
    switch (array->type_id()) {
    case arrow::Type::LARGE_BINARY:
        append_binary_array(down_cast<const arrow::LargeBinaryArray*>(array), start, num_rows, nullable);
        return Status::OK();
    case arrow::Type::BINARY:
        append_binary_array(down_cast<const arrow::BinaryArray*>(array), start, num_rows, nullable);
        return Status::OK();
    default:
        return Status::InternalError(fmt::format(
                "Paimon BLOB column has unexpected arrow type {}, expected large_binary", array->type()->ToString()));
    }
}

} // namespace starrocks
