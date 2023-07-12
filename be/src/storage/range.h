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

#include <algorithm>
#include <initializer_list>
#include <sstream>
#include <string>
#include <vector>

#include "column/datum.h"
#include "storage/rowset/common.h"
#include "util/logging.h"

namespace starrocks {

// Range represent a logical contiguous range of a segment file.
// Range contains a inclusive start row number and an exclusive end row number.
template <typename T = starrocks::rowid_t>
class Range {
    using rowid_t = T;


public:
    Range() = default;
    Range(rowid_t begin, rowid_t end);

    // Enable copy/move ctor and assignment.
    Range(const Range&) = default;
    Range& operator=(const Range&) = default;
    Range(Range&&) = default;
    Range& operator=(Range&&) = default;

    // the id of start row, inclusive.
    rowid_t begin() const { return _begin; }

    // the id of end row, exclusive.
    rowid_t end() const { return _end; }

    bool empty() const { return span_size() == 0; }

    // number of rows covered by this range.
    rowid_t span_size() const { return _end - _begin; }

    // return a new range that represent the intersection of |this| and |r|.
    Range intersection(const Range& r) const;

    bool has_intersection(const Range& rhs) const { return !(_end <= rhs.begin() || rhs.end() <= _begin); }

    bool contains(rowid_t row) const { return row < _end; }

    std::string to_string() const;

private:
    rowid_t _begin{0};
    rowid_t _end{0};
};

template <typename T>
inline Range<T>::Range(rowid_t begin, rowid_t end) : _begin(begin), _end(end) {
    if (_begin >= _end) {
        _begin = 0;
        _end = 0;
    }
}

template <typename T>
inline Range<T> Range<T>::intersection(const Range& r) const {
    if (!has_intersection(r)) {
        return {};
    }
    return {std::max(_begin, r._begin), std::min(_end, r._end)};
}

template <typename T>
inline std::string Range<T>::to_string() const {
    std::stringstream ss;
    ss << "[" << _begin << "," << _end << ")";
    return ss.str();
}

template <typename T>
inline std::ostream& operator<<(std::ostream& os, const Range<T>& range) {
    return (os << range.to_string());
}

template <typename T>
inline bool operator==(const Range<T>& r1, const Range<T>& r2) {
    return r1.begin() == r2.begin() && r1.end() == r2.end();
}

template <typename T>
inline bool operator!=(const Range<T>& r1, const Range<T>& r2) {
    return !(r1 == r2);
}

template <typename T>
class SparseRange;

// SparseRangeIterator used to travel a SparseRange.
template <typename T = starrocks::rowid_t>
class SparseRangeIterator {
    using rowid_t = T;

public:
    SparseRangeIterator() = default;
    explicit SparseRangeIterator(const SparseRange<T>* r);

    SparseRangeIterator(const SparseRangeIterator<T>& iter) = default;

    rowid_t begin() const { return _next_rowid; }

    // Return true iff there are untraversed range, i.e, `next` will return a non-empty range.
    bool has_more() const;

    // Return the next contiguous range contains at most |size| rows.
    // `has_more` must be checked before calling this method.
    Range<T> next(rowid_t size);

    // Return the next discontiguous range contains at most |size| rows
    void next_range(rowid_t size, SparseRange<T>* range);

    // rhs should be a ordered sparse range
    SparseRangeIterator<T> intersection(const SparseRange<T>& rhs, SparseRange<T>* result) const;

    void set_range(SparseRange<T>* range) { _range = range; }

    size_t covered_ranges(size_t size) const;

    rowid_t convert_to_bitmap(uint8_t* bitmap, rowid_t max_size) const;

    void skip(rowid_t size);

private:
    const SparseRange<T>* _range{nullptr};
    size_t _index{0};
    rowid_t _next_rowid{0};
};

// SparseRange represent a set of non-intersected contiguous ranges, or, in other words, represent
// a single non-contiguous range.
template <typename T = starrocks::rowid_t>
class SparseRange {
    using rowid_t = T;

public:
    SparseRange() = default;
    SparseRange(rowid_t begin, rowid_t end) { add(Range<T>(begin, end)); }
    explicit SparseRange(const Range<T> r) { add(r); }
    SparseRange(const std::initializer_list<Range<T>>& ranges) { add(ranges); }

    // true iff there are no sub-range.
    bool empty() const;

    void clear() { _ranges.clear(); }

    // number of sub-ranges
    size_t size() const { return _ranges.size(); }

    // begin of the first sub-range.
    rowid_t begin() const { return _ranges[0].begin(); }

    // end of the last sub-range.
    rowid_t end() const { return _ranges.back().end(); }

    // this method will invalidate iterator.
    void add(const Range<T>& r);

    // this method will invalidate iterator.
    void add(const std::initializer_list<Range<T>>& ranges);

    // number of rows covered by this range. it's the sum of all the sub-ranges span size.
    rowid_t span_size() const;

    // only contains single row or empty
    bool is_single_row_or_empty() const;

    // return a new range that represent the intersection of |this| and |r|.
    SparseRange<T> intersection(const SparseRange<T>& rhs) const;

    SparseRangeIterator<T> new_iterator() const;

    std::string to_string() const;

    bool operator==(const SparseRange<T>& rhs) const;
    bool operator!=(const SparseRange<T>& rhs) const;

    const Range<T>& operator[](size_t idx) const { return _ranges[idx]; }

    SparseRange operator&(const SparseRange& rhs) const { return intersection(rhs); }

    SparseRange operator|(const SparseRange& rhs) const;

    SparseRange& operator&=(const SparseRange& rhs);

    SparseRange& operator|=(const SparseRange& rhs);

private:
    friend class SparseRangeIterator<T>;

    void _add_uncheck(const Range<T>& r);

    std::vector<Range<T>> _ranges;
};
using SparseRangePtr = std::shared_ptr<SparseRange<>>;

template <typename T>
inline void SparseRange<T>::_add_uncheck(const Range<T>& r) {
    if (!r.empty()) {
        _ranges.emplace_back(r);
    }
}

template <typename T>
inline bool SparseRange<T>::empty() const {
    return _ranges.empty();
}

template <typename T>
inline void SparseRange<T>::add(const std::initializer_list<Range<T>>& ranges) {
    for (const auto& r : ranges) {
        add(r);
    }
}

template <typename T>
inline void SparseRange<T>::add(const Range<T>& r) {
    if (r.empty()) {
        return;
    }
    if (_ranges.empty() || _ranges.back().end() < r.begin()) {
        _ranges.emplace_back(r);
        return;
    }
    if (r.end() < _ranges.front().begin()) {
        _ranges.insert(_ranges.begin(), r);
        return;
    }
    std::vector<Range<T>> new_ranges;
    new_ranges.reserve(_ranges.size() + 1);
    size_t i = 0;
    // ranges on the left side of |r| and not contiguous with |r|.
    for (; i < _ranges.size() && _ranges[i].end() < r.begin(); i++) {
        new_ranges.emplace_back(_ranges[i]);
    }
    // ranges that has intersection with |r| or contiguous with |r|.
    if (i < _ranges.size() && _ranges[i].begin() <= r.end()) {
        rowid_t b = std::min(_ranges[i].begin(), r.begin());
        rowid_t e = std::max(_ranges[i].end(), r.end());
        for (i++; i < _ranges.size() && _ranges[i].begin() <= r.end(); i++) {
            e = std::max(e, _ranges[i].end());
        }
        new_ranges.emplace_back(Range{b, e});
    } else {
        new_ranges.emplace_back(r);
    }
    // ranges on the right side of |r| and not contiguous with |r|.
    for (; i < _ranges.size(); i++) {
        new_ranges.emplace_back(_ranges[i]);
    }
    _ranges.swap(new_ranges);
}

template <typename T>
inline typename SparseRange<T>::rowid_t SparseRange<T>::span_size() const {
    rowid_t n = 0;
    for (const auto& r : _ranges) {
        n += r.span_size();
    }
    return n;
}

template <typename T>
inline bool SparseRange<T>::is_single_row_or_empty() const {
    return _ranges.empty() || (_ranges.size() == 1 && _ranges[0].span_size() == 1);
}

template <typename T>
inline SparseRangeIterator<T> SparseRange<T>::new_iterator() const {
    return SparseRangeIterator(this);
}

template <typename T>
inline std::string SparseRange<T>::to_string() const {
    std::stringstream ss;
    ss << "(";
    for (const auto& r : _ranges) {
        ss << r.to_string() << ((&r == &_ranges.back()) ? "" : ", ");
    }
    ss << ")";
    return ss.str();
}

template <typename T>
inline SparseRange<T> SparseRange<T>::intersection(const SparseRange<T>& rhs) const {
    SparseRange result;
    for (const auto& r1 : _ranges) {
        for (const auto& r2 : rhs._ranges) {
            if (r1.has_intersection(r2)) {
                result._add_uncheck(r1.intersection(r2));
            }
        }
    }
    return result;
}

template <typename T>
inline SparseRange<T> SparseRange<T>::operator|(const SparseRange<T>& rhs) const {
    SparseRange res = *this;
    res |= rhs;
    return res;
}

template <typename T>
inline SparseRange<T>& SparseRange<T>::operator&=(const SparseRange<T>& rhs) {
    SparseRange<T> tmp = this->intersection(rhs);
    *this = std::move(tmp);
    return *this;
}

template <typename T>
inline SparseRange<T>& SparseRange<T>::operator|=(const SparseRange<T>& rhs) {
    for (size_t i = 0; i < rhs.size(); i++) {
        add(rhs[i]);
    }
    return *this;
}

template <typename T>
inline SparseRangeIterator<T>::SparseRangeIterator(const SparseRange<T>* r) : _range(r) {
    if (!_range->_ranges.empty()) {
        _next_rowid = _range->_ranges[0].begin();
    }
}

template <typename T>
inline bool SparseRangeIterator<T>::has_more() const {
    return _index < _range->_ranges.size();
}

template <typename T>
inline Range<T> SparseRangeIterator<T>::next(SparseRangeIterator<T>::rowid_t size) {
    const std::vector<Range<T>>& ranges = _range->_ranges;
    const Range<T>& range = ranges[_index];
    size = std::min<rowid_t>(size, range.end() - _next_rowid);
    Range<T> ret(_next_rowid, _next_rowid + size);
    _next_rowid += size;
    if (_next_rowid == range.end()) {
        ++_index;
        if (_index < ranges.size()) {
            _next_rowid = ranges[_index].begin();
        }
    }
    return ret;
}

template <typename T>
inline void SparseRangeIterator<T>::next_range(SparseRangeIterator<T>::rowid_t size, SparseRange<T>* range) {
    while (size > 0 && has_more()) {
        Range r = next(size);
        range->add(r);
        size -= r.span_size();
    }
}

template <typename T>
inline SparseRangeIterator<T> SparseRangeIterator<T>::intersection(const SparseRange<T>& rhs, SparseRange<T>* result) const {
    DCHECK(std::is_sorted(rhs._ranges.begin(), rhs._ranges.end(),
                          [](const auto& l, const auto& r) { return l.begin() < r.begin(); }));
    for (size_t i = _index; i < _range->_ranges.size(); ++i) {
        const auto& r1 = _range->_ranges[i];
        for (const auto& r2 : rhs._ranges) {
            if (r1.end() < r2.begin()) {
                break;
            }
            if (r1.has_intersection(r2)) {
                result->_add_uncheck(r1.intersection(r2));
            }
        }
    }
    SparseRangeIterator<T> res(result);
    if (res.has_more()) {
        for (size_t i = 0; i < res._range->size(); ++i) {
            // set idx and next rowid
            if (_next_rowid < res._range->_ranges[i].end()) {
                res._next_rowid = std::max(res._range->_ranges[i].begin(), _next_rowid);
                res._index = i;
                break;
            }
            // filter all range
            if (i == res._range->size() - 1) {
                res._index = res._range->size();
            }
        }
    }
    return res;
}

template <typename T>
inline size_t SparseRangeIterator<T>::covered_ranges(size_t size) const {
    if (size == 0) {
        return 0;
    }
    const std::vector<Range<T>>& ranges = _range->_ranges;
    rowid_t end = std::min<rowid_t>(_next_rowid + size, ranges.back().end());
    size_t i = _index;
    for (; ranges[i].end() < end; i++) {
    }
    i += (end > ranges[i].begin());
    return i - _index;
}

template <typename T>
inline void SparseRangeIterator<T>::skip(SparseRangeIterator<T>::rowid_t size) {
    _next_rowid += size;
    const std::vector<Range<T>>& ranges = _range->_ranges;
    while (_index < ranges.size() && ranges[_index].end() <= _next_rowid) {
        _index++;
    }
    if (_index < ranges.size()) {
        _next_rowid = std::max(_next_rowid, ranges[_index].begin());
    }
}

template <typename T>
inline typename SparseRangeIterator<T>::rowid_t SparseRangeIterator<T>::convert_to_bitmap(uint8_t* bitmap, rowid_t max_size) const {
    rowid_t curr_row = _next_rowid;
    size_t index = _index;
    const std::vector<Range<T>>& ranges = _range->_ranges;
    max_size = std::min<rowid_t>(max_size, ranges.back().end() - _next_rowid);
    DCHECK(!has_more() || ranges[_index].contains(curr_row));
    for (rowid_t i = 0; i < max_size; i++) {
        rowid_t b = ranges[index].begin();
        rowid_t e = ranges[index].end();
        bitmap[i] = (curr_row - b) < (e - b);
        curr_row++;
        index += (curr_row == e);
    }
    return max_size;
}

template <typename T>
inline bool SparseRange<T>::operator==(const SparseRange<T>& rhs) const {
    return _ranges == rhs._ranges;
}

template <typename T>
inline bool SparseRange<T>::operator!=(const SparseRange<T>& rhs) const {
    return !(*this == rhs);
}

template <typename T>
inline std::ostream& operator<<(std::ostream& os, const SparseRange<T>& range) {
    return (os << range.to_string());
}

template class Range<>;
template class Range<starrocks::ordinal_t>;

template class SparseRange<>;
template class SparseRange<starrocks::ordinal_t>;

template class SparseRangeIterator<>;
template class SparseRangeIterator<starrocks::ordinal_t>;

} // namespace starrocks
