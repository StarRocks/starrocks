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

#include "formats/csv/varbinary_converter.h"

#include <iostream>

#include "column/binary_column.h"
#include "common/config.h"
#include "exprs/base64.h"
#include "gutil/strings/escaping.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/defer_op.h"
#include "util/string_parser.hpp"

namespace starrocks::csv {

Status VarBinaryConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                        const Options& options) const {
    auto* binary = down_cast<const BinaryColumn*>(&column);
    auto& bytes = binary->get_bytes();
    auto& offsets = binary->get_offset();
    // TODO: support binary type config later

    Slice str(&bytes[offsets[row_num]], offsets[row_num + 1] - offsets[row_num]);
    auto byte_length = offsets[row_num + 1] - offsets[row_num];

    int buf_size = (size_t)(4.0 * ceil((double)byte_length / 3.0)) + 1;
    auto buf = new uint8_t[buf_size];
    DeferOp defer([&]() { delete[] buf; });

    int encoded_len = base64_encode2((unsigned char*)str.data, byte_length, buf);
    return os->write(Slice(buf, encoded_len));
}

Status VarBinaryConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                               const Options& options) const {
    RETURN_IF_ERROR(os->write('"'));
    RETURN_IF_ERROR(write_string(os, column, row_num, options));
    return os->write('"');
}

bool VarBinaryConverter::read_string(Column* column, const Slice& s, const Options& options) const {
    int max_size = -1;
    if (options.type_desc != nullptr) {
        max_size = options.type_desc->len;
    }

    char* data_ptr = static_cast<char*>(s.data);
    int start = StringParser::skip_leading_whitespace(data_ptr, s.size);
    if (start == s.size) {
        column->append_default();
        return true;
    }

    auto buf = new char[s.size + 3];
    DeferOp defer([&]() { delete[] buf; });

    int r = base64_decode2(s.data + start, s.size, buf);
    char* data = buf;
    int len = r;

    if (r == -1) { // fallback if decode failed
        data = data_ptr;
        len = s.size;
    }

    bool length_check_status = true;
    // check if length exceed max varbinary length
    if (options.is_hive) {
        if (UNLIKELY(max_size != -1 && len > max_size)) {
            length_check_status = false;
        }
    } else {
        if (config::enable_check_string_lengths &&
            ((len > TypeDescriptor::MAX_VARCHAR_LENGTH) || (max_size != -1 && len > max_size))) {
            length_check_status = false;
        }
    }
    if (!length_check_status) {
        LOG(WARNING) << "Column [" << column->get_name() << "]'s length exceed max varbinary length.";
        return false;
    }

    down_cast<BinaryColumn*>(column)->append(Slice(data, len));
    return true;
}

bool VarBinaryConverter::read_quoted_string(Column* column, const Slice& tmp_s, const Options& options) const {
    Slice s = tmp_s;
    // TODO: need write quote for binary?
    if (!remove_enclosing_quotes<'"'>(&s)) {
        return false;
    }
    return read_string(column, s, options);
}

} // namespace starrocks::csv
