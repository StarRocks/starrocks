// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <rapidjson/document.h>

#include "column/column_builder.h"
#include "exprs/vectorized/builtin_functions.h"
#include "exprs/vectorized/function_helper.h"

namespace starrocks {
namespace vectorized {

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

} // namespace vectorized
} // namespace starrocks
