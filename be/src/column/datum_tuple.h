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

#include <vector>

#include "column/datum.h"

namespace starrocks {

class Schema;

class DatumTuple {
public:
    DatumTuple() = default;
    explicit DatumTuple(std::vector<Datum> datums) : _datums(std::move(datums)) {}

    size_t size() const { return _datums.size(); }

    void reserve(size_t n) { _datums.reserve(n); }

    void append(const Datum& datum) { _datums.emplace_back(datum); }

    const Datum& operator[](size_t n) const { return _datums[n]; }

    const Datum& get(size_t n) const { return _datums.at(n); }

    Datum& operator[](size_t n) { return _datums[n]; }

    Datum& get(size_t n) { return _datums.at(n); }

    int compare(const Schema& schema, const DatumTuple& rhs) const;

    const std::vector<Datum>& datums() const { return _datums; }

private:
    std::vector<Datum> _datums;
};

} // namespace starrocks
