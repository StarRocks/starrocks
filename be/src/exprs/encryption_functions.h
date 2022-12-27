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

#include <rapidjson/document.h>

#include "column/column_builder.h"
#include "exprs/builtin_functions.h"
#include "exprs/function_helper.h"

namespace starrocks {

class EncryptionFunctions {
public:
    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: Int32Column
     */
    DEFINE_VECTORIZED_FN(aes_encrypt);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: DoubleColumn
     */
    DEFINE_VECTORIZED_FN(aes_decrypt);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(from_base64);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: Int32Column
     */
    DEFINE_VECTORIZED_FN(to_base64);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: Int32Column
     */
    DEFINE_VECTORIZED_FN(md5sum);
    DEFINE_VECTORIZED_FN(md5sum_numeric);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: Int32Column
     */
    DEFINE_VECTORIZED_FN(md5);

    /*
     * Called by sha2 to the corresponding part
     */
    DEFINE_VECTORIZED_FN(sha224);
    DEFINE_VECTORIZED_FN(sha256);
    DEFINE_VECTORIZED_FN(sha384);
    DEFINE_VECTORIZED_FN(sha512);
    DEFINE_VECTORIZED_FN(invalid_sha);
    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: Int32Column
     */
    DEFINE_VECTORIZED_FN(sha2);
    static Status sha2_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status sha2_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    // method for sha2
    struct SHA2Ctx {
        ScalarFunction function;
    };
};

} // namespace starrocks
