// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <vector>

#include "column/datum.h"

namespace starrocks::vectorized {

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

} // namespace starrocks::vectorized
