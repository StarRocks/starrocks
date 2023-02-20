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

#include "exprs/function_helper.h"

namespace starrocks {
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

} // namespace starrocks
