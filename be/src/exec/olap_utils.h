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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/olap_utils.h

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

#pragma once

#include <cmath>

#include "column/type_traits.h"
#include "common/logging.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Opcodes_types.h"
#include "runtime/datetime_value.h"
#include "storage/tuple.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks {

static const char* NEGATIVE_INFINITY = "-oo";
static const char* POSITIVE_INFINITY = "+oo";

typedef struct OlapScanRange {
public:
    OlapScanRange() {
        begin_scan_range.add_value(NEGATIVE_INFINITY);
        end_scan_range.add_value(POSITIVE_INFINITY);
    }
    OlapScanRange(bool begin, bool end, const std::vector<std::string>& begin_range,
                  const std::vector<std::string>& end_range)
            : begin_include(begin), end_include(end), begin_scan_range(begin_range), end_scan_range(end_range) {}

    bool begin_include{true};
    bool end_include{true};
    OlapTuple begin_scan_range;
    OlapTuple end_scan_range;
} OlapScanRange;

enum SQLFilterOp {
    FILTER_LARGER = 0,
    FILTER_LARGER_OR_EQUAL = 1,
    FILTER_LESS = 2,
    FILTER_LESS_OR_EQUAL = 3,
    FILTER_IN = 4,
    FILTER_NOT_IN = 5
};

inline SQLFilterOp to_olap_filter_type(TExprOpcode::type type, bool opposite) {
    switch (type) {
    case TExprOpcode::LT:
        return opposite ? FILTER_LARGER : FILTER_LESS;
    case TExprOpcode::LE:
        //TODO: Datetime may be truncated to a date column, so we convert LT to LE,
        // for example: '2010-01-01 00:00:01' will be truncate to '2010-01-01'
        return opposite ? FILTER_LARGER_OR_EQUAL : FILTER_LESS_OR_EQUAL;
    case TExprOpcode::GT:
        return opposite ? FILTER_LESS : FILTER_LARGER;

    case TExprOpcode::GE:
        return opposite ? FILTER_LESS_OR_EQUAL : FILTER_LARGER_OR_EQUAL;

    case TExprOpcode::EQ:
        return opposite ? FILTER_NOT_IN : FILTER_IN;

    case TExprOpcode::NE:
        return opposite ? FILTER_IN : FILTER_NOT_IN;

    case TExprOpcode::EQ_FOR_NULL:
        return FILTER_IN;

    default:
        VLOG(1) << "TExprOpcode: " << type;
        DCHECK(false);
    }

    return FILTER_IN;
}

} // namespace starrocks
