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

#include <iomanip>
#include <iostream>
#include <sstream>

#include "column/binary_column.h"
#include "gutil/strings/escaping.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/string_parser.hpp"

namespace starrocks::csv {

Status VarBinaryConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                        const Options& options) const {
    auto* binary = down_cast<const BinaryColumn*>(&column);
    auto& bytes = binary->get_bytes();
    auto& offsets = binary->get_offset();
    // TODO: support binary type config later

    Slice str(&bytes[offsets[row_num]], offsets[row_num + 1] - offsets[row_num]);
    std::stringstream ss;
    ss << std::hex << std::uppercase << std::setfill('0');
    for (int i = 0; i < str.size; ++i) {
        // setw is not sticky. stringstream only converts integral values,
        // so a cast to int is required, but only convert the least significant byte to hex.
        ss << std::setw(2) << (static_cast<int32_t>(str.data[i]) & 0xFF);
    }
    // from binary to hex
    return os->write(Slice(ss.str()));
}

Status VarBinaryConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                               const Options& options) const {
    RETURN_IF_ERROR(os->write('"'));
    RETURN_IF_ERROR(write_string(os, column, row_num, options));
    return os->write('"');
}

bool VarBinaryConverter::read_string(Column* column, const Slice& s, const Options& options) const {
    int max_size = 0;
    if (options.type_desc != nullptr) {
        max_size = options.type_desc->len;
    }

    char* data_ptr = static_cast<char*>(s.data);
    int start = StringParser::skip_leading_whitespace(data_ptr, s.size);
    int len = s.size - start;
    // hex's length should be 2x.
    if (len % 2 != 0) {
        LOG(WARNING) << "Column [" << column->get_name() << "]'s length invalid:" << s.to_string();
        return false;
    }

    // hex's length should not greater than MAX_VARCHAR_LENGTH.
    int hex_len = len / 2;
    if (UNLIKELY((hex_len > TypeDescriptor::MAX_VARCHAR_LENGTH) || (max_size > 0 && hex_len > max_size))) {
        LOG(WARNING) << "Column [" << column->get_name() << "]'s length exceed max varbinary length.";
        return false;
    }

    // check slice is valid
    for (int i = start; i < s.size; i++) {
        if (LIKELY((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'A' && s[i] <= 'F') || (s[i] >= 'a' && s[i] <= 'f'))) {
            continue;
        } else {
            LOG(WARNING) << "Invalid input's not a legal hex-encoded value:" << s.to_string();
            return false;
        }
    }

    // from string to binary
    std::unique_ptr<char[]> p;
    p.reset(new char[hex_len]);
    strings::a2b_hex(data_ptr + start, p.get(), hex_len);
    down_cast<BinaryColumn*>(column)->append(Slice(p.get(), hex_len));

    return true;
}

bool VarBinaryConverter::read_quoted_string(Column* column, const Slice& s, const Options& options) const {
    // TODO: need write quote for binary?
    if (!remove_enclosing_quotes<'"'>(&s)) {
        return false;
    }
    return read_string(column, s, options);
}

} // namespace starrocks::csv
