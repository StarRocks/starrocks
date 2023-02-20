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

#include <utility>

#include "column/datum.h"

namespace starrocks {

class ZoneMapDetail {
public:
    // ctors
    ZoneMapDetail() = default;
    ZoneMapDetail(const Datum& min_or_null_value, Datum max_value)
            : _has_null(min_or_null_value.is_null()),
              _min_value(min_or_null_value),
              _max_value(std::move(std::move(max_value))),
              _num_rows(0) {}
    ZoneMapDetail(Datum min_value, Datum max_value, bool has_null)
            : _has_null(has_null),
              _min_value(std::move(std::move(min_value))),
              _max_value(std::move(std::move(max_value))),
              _num_rows(0) {}

    // methods
    bool has_null() const { return _has_null; }
    void set_has_null(bool v) { _has_null = v; }
    bool has_not_null() const { return !_min_value.is_null() || !_max_value.is_null(); }
    Datum& min_value() { return _min_value; }
    const Datum& min_value() const { return _min_value; }
    Datum& max_value() { return _max_value; }
    const Datum& max_value() const { return _max_value; }
    const Datum& min_or_null_value() const {
        if (_has_null) return _null_value;
        return _min_value;
    }
    void set_num_rows(const size_t num_rows) { _num_rows = num_rows; }
    size_t num_rows() const { return _num_rows; }

private:
    bool _has_null;
    Datum _null_value;
    Datum _min_value;
    Datum _max_value;
    size_t _num_rows;
};
} // namespace starrocks
