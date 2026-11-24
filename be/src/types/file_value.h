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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>

#include "base/statusor.h"
#include "base/string/slice.h"
#include "types/datum.h"

namespace starrocks {
namespace file_field {

// Field positions shared by the FILE column and its DatumStruct representation.
enum Index : std::size_t {
    URI = 0,
    OFFSET = 1,
    SIZE = 2,
    CONTENT_TYPE = 3,
    CHECKSUM = 4,
    INLINE = 5,
    NUM_FIELDS = 6,
};

} // namespace file_field

// The slices borrow their bytes from the underlying FILE storage, which must outlive this view.
struct FileRowRef {
    std::optional<Slice> uri;
    std::optional<int64_t> offset;
    std::optional<int64_t> size;
    std::optional<Slice> content_type;
    std::optional<Slice> checksum;
    std::optional<Slice> inline_bytes;

    static StatusOr<FileRowRef> from_datum(const Datum& datum) {
        try {
            const auto& fields = datum.get_struct();
            if (fields.size() != file_field::NUM_FIELDS) {
                return Status::InvalidArgument("Invalid FILE field count");
            }
            return FileRowRef{.uri = read_field<Slice>(fields[file_field::URI]),
                              .offset = read_field<int64_t>(fields[file_field::OFFSET]),
                              .size = read_field<int64_t>(fields[file_field::SIZE]),
                              .content_type = read_field<Slice>(fields[file_field::CONTENT_TYPE]),
                              .checksum = read_field<Slice>(fields[file_field::CHECKSUM]),
                              .inline_bytes = read_field<Slice>(fields[file_field::INLINE])};
        } catch (const std::bad_variant_access&) {
            return Status::InvalidArgument("Invalid FILE datum type");
        }
    }

private:
    template <typename T>
    static std::optional<T> read_field(const Datum& field) {
        if (field.is_null()) return std::nullopt;
        return field.get<T>();
    }
};

} // namespace starrocks
