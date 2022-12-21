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

#include <glog/logging.h> // CHECK_EQ

#include <sstream>
#include <string>
#include <vector>

#include "storage/seek_tuple.h"

namespace starrocks {

class SeekRange {
public:
    SeekRange() = default;
    // default non-inclusive lower and non-inclusive upper.
    SeekRange(SeekTuple lower, SeekTuple upper) : _lower(std::move(lower)), _upper(std::move(upper)) {}

    void set_inclusive_lower(bool inc) { _inc_lower = inc; }
    void set_inclusive_upper(bool inc) { _inc_upper = inc; }

    bool inclusive_lower() const { return _inc_lower; }
    bool inclusive_upper() const { return _inc_upper; }

    const SeekTuple& lower() const { return _lower; }
    const SeekTuple& upper() const { return _upper; }

    void convert_to(SeekRange* dst, const std::vector<LogicalType>& new_types) const {
        dst->_inc_lower = _inc_lower;
        dst->_inc_upper = _inc_upper;
        _lower.convert_to(&dst->_lower, new_types);
        _upper.convert_to(&dst->_upper, new_types);
    }

    std::string debug_string() const {
        std::stringstream ss;
        if (_inc_lower) {
            ss << "[";
        } else {
            ss << "(";
        }
        ss << _lower.debug_string() << ", " << _upper.debug_string();
        if (_inc_upper) {
            ss << "]";
        } else {
            ss << ")";
        }
        return ss.str();
    }

private:
    bool _inc_lower = false;
    bool _inc_upper = false;
    SeekTuple _lower;
    SeekTuple _upper;
};

} // namespace starrocks
