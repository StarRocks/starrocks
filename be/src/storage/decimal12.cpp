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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/decimal12.cpp

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

#include "storage/decimal12.h"

#include "storage/utils.h"

namespace starrocks {

Status decimal12_t::from_string(const std::string& str) {
    integer = 0;
    fraction = 0;
    const char* value_string = str.c_str();
    const char* sign = strchr(value_string, '-');

    if (sign != nullptr) {
        if (sign != value_string) {
            return Status::InvalidArgument("Fail to cast to decimal.");
        } else {
            ++value_string;
        }
    }

    const char* sepr = strchr(value_string, '.');
    if ((sepr != nullptr && sepr - value_string > MAX_INT_DIGITS_NUM) ||
        (sepr == nullptr && strlen(value_string) > MAX_INT_DIGITS_NUM)) {
        integer = 999999999999999999;
        fraction = 999999999;
    } else {
        DIAGNOSTIC_PUSH
#if defined(__clang__)
        DIAGNOSTIC_IGNORE("-Waddress-of-packed-member")
#endif
        if (sepr == value_string) {
            sscanf(value_string, ".%9d", &fraction);
            integer = 0;
        } else {
            sscanf(value_string, "%18ld.%9d", &integer, &fraction);
        }
        DIAGNOSTIC_POP

        int32_t frac_len = (nullptr != sepr) ? MAX_FRAC_DIGITS_NUM - strlen(sepr + 1) : MAX_FRAC_DIGITS_NUM;
        frac_len = frac_len > 0 ? frac_len : 0;
        fraction *= g_power_table[frac_len];
    }

    if (sign != nullptr) {
        fraction = -fraction;
        integer = -integer;
    }

    return Status::OK();
}

} // namespace starrocks
