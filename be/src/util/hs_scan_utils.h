// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/timezone_utils.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

#pragma once

#include <string.h>
#include <hs/hs.h>
#include "udf/udf.h"

namespace starrocks {
class HsScanUtils {
public:
    const std::string hs_reg_pattern = "[+|-]{1}\\d{2}\\:\\d{2}";
    const int HS_SCAN_SUCCESS = 1;
    const int HS_SCAN_NO_RESULT = 0;

    Status compile() {
        if (hs_compile(hs_reg_pattern.c_str(), HS_FLAG_ALLOWEMPTY | HS_FLAG_DOTALL | HS_FLAG_UTF8 | HS_FLAG_SINGLEMATCH,
                       HS_MODE_BLOCK, NULL, &database, &compile_err) != HS_SUCCESS) {
            std::stringstream error;
            error << "Invalid regex expression: " << hs_reg_pattern << ": " << compile_err->message;
            hs_free_compile_error(compile_err);
            return Status::InvalidArgument(error.str());
        }

        if (hs_alloc_scratch(database, &scratch) != HS_SUCCESS) {
            std::stringstream error;
            error << "ERROR: Unable to allocate scratch space.";
            hs_free_database(database);
            return Status::InvalidArgument(error.str());
        }

        return Status::OK();
    }

    int scan(std::string timezone) {
        bool v = false;
        auto status = hs_scan(
        database, timezone.c_str(), timezone.size(), 0, scratch,
        [](unsigned int id, unsigned long long from, unsigned long long to, unsigned int flags,
           void* ctx) -> int {
            *((bool*)ctx) = true;
            return 1;
        },
        &v);
        return status;
    }

    HsScanUtils() {}
    ~HsScanUtils() {
        if (scratch != nullptr) {
            hs_free_scratch(scratch);
        }

        if (database != nullptr) {
            hs_free_database(database);
        }
    }
//private:
    hs_database_t* database = nullptr;
    hs_compile_error_t* compile_err = nullptr;
    hs_scratch_t* scratch = nullptr;
};
} // namespace starrocks

