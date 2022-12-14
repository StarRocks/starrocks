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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/olap_common.h

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

#include "storage/olap_common.h"

#include "util/string_parser.hpp"

namespace starrocks {

void RowsetId::init(std::string_view rowset_id_str) {
    // for new rowsetid its a 48 hex string
    // if the len < 48, then it is an old format rowset id
    if (rowset_id_str.length() < 48) {
        StringParser::ParseResult result;
        auto high = StringParser::string_to_int<int64_t>(rowset_id_str.data(), rowset_id_str.size(), &result);
        DCHECK_EQ(StringParser::PARSE_SUCCESS, result);
        init(1, high, 0, 0);
    } else {
        int64_t high = 0;
        int64_t middle = 0;
        int64_t low = 0;
        from_hex(&high, rowset_id_str.substr(0, 16));
        from_hex(&middle, rowset_id_str.substr(16, 16));
        from_hex(&low, rowset_id_str.substr(32, 16));
        init(high >> 56, high & LOW_56_BITS, middle, low);
    }
}

} // namespace starrocks
