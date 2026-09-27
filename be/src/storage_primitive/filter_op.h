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

#include "common/logging.h"
#include "gen_cpp/Opcodes_types.h"

namespace starrocks {

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
        VLOG(2) << "TExprOpcode: " << type;
        DCHECK(false);
    }

    return FILTER_IN;
}

inline SQLFilterOp invert_olap_filter_type(const SQLFilterOp op) {
    switch (op) {
    case FILTER_LARGER:
        return FILTER_LESS_OR_EQUAL;
    case FILTER_LARGER_OR_EQUAL:
        return FILTER_LESS;
    case FILTER_LESS:
        return FILTER_LARGER_OR_EQUAL;
    case FILTER_LESS_OR_EQUAL:
        return FILTER_LARGER;
    case FILTER_IN:
        return FILTER_NOT_IN;
    case FILTER_NOT_IN:
        return FILTER_IN;
    default:
        VLOG(2) << "Unkown SQLFilterOp when inverting it: " << op;
        DCHECK(false);
    }
    return FILTER_IN;
}

template <bool Negative>
SQLFilterOp to_olap_filter_type(TExprOpcode::type type, bool opposite) {
    const auto op = to_olap_filter_type(type, opposite);
    if constexpr (Negative) {
        return invert_olap_filter_type(op);
    } else {
        return op;
    }
}

} // namespace starrocks
