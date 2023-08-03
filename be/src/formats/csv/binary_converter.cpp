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

#include "formats/csv/binary_converter.h"

#include "column/binary_column.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"

namespace starrocks::csv {

Status BinaryConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                     const Options& options) const {
    auto* binary = down_cast<const BinaryColumn*>(&column);
    auto& bytes = binary->get_bytes();
    auto& offsets = binary->get_offset();

    Slice s(&bytes[offsets[row_num]], offsets[row_num + 1] - offsets[row_num]);
    // TODO(zhuming): escape delimiter characters.
    return os->write(s);
}

Status BinaryConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                            const Options& options) const {
    auto* binary = down_cast<const BinaryColumn*>(&column);
    auto& bytes = binary->get_bytes();
    auto& offsets = binary->get_offset();

    Slice s(&bytes[offsets[row_num]], offsets[row_num + 1] - offsets[row_num]);
    // TODO(zhuming): escape delimiter characters.
    RETURN_IF_ERROR(os->write('"'));
    char* quota = nullptr;
    // Escape double quota.
    while ((quota = (char*)memchr(s.data, '"', s.size)) != nullptr) {
        size_t len = quota + 1 - s.data;
        RETURN_IF_ERROR(os->write(Slice(s.data, len)));
        RETURN_IF_ERROR(os->write('"'));
        s.remove_prefix(len);
    }
    RETURN_IF_ERROR(os->write(s));
    return os->write('"');
}

bool BinaryConverter::read_string(Column* column, Slice s, const Options& options) const {
    int max_size = 0;
    if (options.type_desc != nullptr) {
        max_size = options.type_desc->len;
    }

    if (UNLIKELY((s.size > TypeDescriptor::MAX_VARCHAR_LENGTH) || (max_size > 0 && s.size > max_size))) {
        VLOG(3) << strings::Substitute("Column [$0]'s length exceed max varchar length. str_size($1), max_size($2)",
                                       column->get_name(), s.size, max_size);
        return false;
    }
    down_cast<BinaryColumn*>(column)->append(s);
    return true;
}

bool BinaryConverter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    if (!remove_enclosing_quotes<'"'>(&s)) {
        return false;
    }

    auto* binary = down_cast<BinaryColumn*>(column);
    auto& bytes = binary->get_bytes();
    auto& offsets = binary->get_offset();
    auto old_size = bytes.size();
    DCHECK_EQ(old_size, offsets.back());

    char* const end = s.data + s.size;
    while (s.size > 0) {
        char* next_pos = (char*)memchr(s.data, '"', s.size);
        if (next_pos == nullptr) {
            bytes.insert(bytes.end(), s.data, s.data + s.size);
            break;
        } else {
            ++next_pos;
            bytes.insert(bytes.end(), s.data, next_pos);
            // If there are two successive double quotes.
            if (next_pos != end && *next_pos == '"') {
                next_pos++;
            }
            s.data = next_pos;
            s.size = end - next_pos;
        }
    }
    auto new_size = bytes.size();
    binary->invalidate_slice_cache();

    int max_size = 0;
    if (options.type_desc != nullptr) {
        max_size = options.type_desc->len;
    }
    size_t ext_size = new_size - old_size;
    if (UNLIKELY((ext_size > TypeDescriptor::MAX_VARCHAR_LENGTH) || (max_size > 0 && ext_size > max_size))) {
        bytes.resize(old_size);
        VLOG(3) << strings::Substitute(
                "Column [$0]'s length exceed max varchar length. old_size($1), new_size($2), ext_size($3), "
                "max_size($4)",
                column->get_name(), old_size, new_size, ext_size, max_size);
        return false;
    }
    offsets.push_back(bytes.size());
    return true;
}

} // namespace starrocks::csv
