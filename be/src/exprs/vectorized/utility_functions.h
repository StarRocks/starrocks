// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exprs/vectorized/function_helper.h"

namespace starrocks {
namespace vectorized {
class UtilityFunctions {
public:
    /**
     * return version.
     */
    DEFINE_VECTORIZED_FN(version);
    DEFINE_VECTORIZED_FN(current_version);

    /**
     * sleep for int seconds.
     * @param: [int]
     * @return true
     */
    DEFINE_VECTORIZED_FN(sleep);

    /**
     * return last query id 
     */
    DEFINE_VECTORIZED_FN(last_query_id);

    /**
     * returns uuid.
     */
    DEFINE_VECTORIZED_FN(uuid);

    /**
     * Returns an approximate UUID.
     * timestamp(64bit) + backend_id(32bit: hash(ip) ^ port) + rand (16bit) +
     * tid(thread id 32 bit) + i (increment 16 bit)
     */
    DEFINE_VECTORIZED_FN(uuid_numeric);

    /**
     * assert whether input is true
     * returns true if input is true
     * report runtime error if input is false
     */
    DEFINE_VECTORIZED_FN(assert_true);
    /**
     * return the host name
     */
    DEFINE_VECTORIZED_FN(host_name);
};

} // namespace vectorized
} // namespace starrocks
