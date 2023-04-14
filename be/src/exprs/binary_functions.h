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

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"

namespace starrocks {

enum class BinaryFormatType {
    HEX = 0,
    ENCODE64 = 1,
    UTF8 = 2,
};

struct BinaryFormatState {
    BinaryFormatType to_binary_type;

    static BinaryFormatType to_binary_format(const std::string& s) {
        std::string str(s);
        std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) { return std::tolower(c); });
        if (str == "encode64") {
            return BinaryFormatType::ENCODE64;
        } else if (str == "utf8") {
            return BinaryFormatType::UTF8;
        } else {
            // by default
            return BinaryFormatType::HEX;
        }
    }
};

class BinaryFunctions {
public:
    // to_binary
    DEFINE_VECTORIZED_FN(to_binary);
    static Status to_binary_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status to_binary_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    // from_binary
    DEFINE_VECTORIZED_FN(from_binary);
    static Status from_binary_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status from_binary_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);
};

} // namespace starrocks
