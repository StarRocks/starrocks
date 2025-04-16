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
#include "column/vectorized_fwd.h"
#include "storage/rowset/common.h"
#include "util/logging.h"

namespace starrocks {

// Range represents a logical contiguous range of a segment file.
// Range contains an inclusive start row number and an exclusive end row number.
template <typename T = rowid_t>
class Range {
public:
    Range() = default;
    Range(T begin, T end);

    // Enable copy/move ctor and assignment.
    Range(const Range&) = default;
    Range& operator=(const Range&) = default;
    Range(Range&&) = default;
    Range& operator=(Range&&) = default;

    // the id of start row, inclusive.
    T begin() const { return _begin; }

    // the id of end row, exclusive.
    T end() const { return _end; }

    bool empty() const { return span_size() == 0; }

    // number of rows covered by this range.
    T span_size() const { return _end - _begin; }

    // return a new range that represent the intersection of |this| and |r|.
    Range intersection(const Range& r) const;

    Range filter(const Filter* const filter) const;

    bool has_intersection(const Range& rhs) const { return !(_end <= rhs.begin() || rhs.end() <= _begin); }

    bool contains(T row) const { return row < _end; }

    std::string to_string() const;

private:
    T _begin{0};
    T _end{0};
};

template <typename T>
inline Range<T>::Range(T begin, T end) : _begin(begin), _end(end) {
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
inline Range<T> Range<T>::filter(const Filter* const filter) const {
    DCHECK(span_size() == filter->size());
    const int32_t len = filter->size();
    int32_t start = len;
    int32_t end = -1;
    for (int32_t i = 0; i < len; i++) {
        if (filter->data()[i] == 1) {
            start = i;
            break;
        }
    }

    for (int32_t i = len - 1; i >= 0; i--) {
        if (filter->data()[i] == 1) {
            end = i;
            break;
        }
    }
    return start <= end ? Range<T>(_begin + start, _begin + end + 1) : Range<T>(_begin, _begin);
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
template <typename T = rowid_t>
class SparseRangeIterator {
public:
    SparseRangeIterator() = default;
    explicit SparseRangeIterator(const SparseRange<T>* r);

    SparseRangeIterator(const SparseRangeIterator<T>& iter) = default;

    T begin() const { return _next_rowid; }

    // Return true if there are untraveled range, i.e, `next` will return a non-empty range.
    bool has_more() const;

    // Return the next contiguous range contains at most |size| rows.
    // `has_more` must be checked before calling this method.
    Range<T> next(T size);

    // Return the next discontinuous range contains at most |size| rows
    void next_range(T size, SparseRange<T>* range);

    // rhs should be an ordered sparse range
    SparseRangeIterator<T> intersection(const SparseRange<T>& rhs, SparseRange<T>* result) const;

    void set_range(SparseRange<T>* range) { _range = range; }

    size_t covered_ranges(size_t size) const;

    void skip(T size);

private:
    const SparseRange<T>* _range{nullptr};
    size_t _index{0};
    T _next_rowid{0};
};

// SparseRange represent a set of non-intersected contiguous ranges, or, in other words, represent
// a single non-contiguous range.
template <typename T = rowid_t>
class SparseRange {
public:
    SparseRange() = default;
    SparseRange(T begin, T end) { add(Range<T>(begin, end)); }
    explicit SparseRange(const Range<T> r) { add(r); }
    SparseRange(const std::initializer_list<Range<T>>& ranges) { add(ranges); }

    // true iff there are no sub-range.
    bool empty() const;

    void clear() { _ranges.clear(); }

    // number of sub-ranges
    size_t size() const { return _ranges.size(); }

    // begin of the first sub-range.
    T begin() const { return _ranges[0].begin(); }

    // end of the last sub-range.
    T end() const { return _ranges.back().end(); }

    // this method will invalidate iterator.
    void add(const Range<T>& r);

    // this method will invalidate iterator.
    void add(const std::initializer_list<Range<T>>& ranges);

    // number of rows covered by this range. it's the sum of all the sub-ranges span size.
    T span_size() const;

    // return a new range that represent the intersection of |this| and |r|.
    SparseRange<T> intersection(const SparseRange<T>& rhs) const;

    SparseRangeIterator<T> new_iterator() const;

    std::string to_string() const;

    void split_and_reverse(size_t expected_range_cnt, size_t chunk_size);

    bool is_sorted() const { return _is_sorted; }
    void set_sorted(bool normalized) { _is_sorted = normalized; }

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
    bool _is_sorted = true;
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
    // ranges that have intersection with |r| or contiguous with |r|.
    if (i < _ranges.size() && _ranges[i].begin() <= r.end()) {
        T b = std::min(_ranges[i].begin(), r.begin());
        T e = std::max(_ranges[i].end(), r.end());
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
inline T SparseRange<T>::span_size() const {
    T n = 0;
    for (const auto& r : _ranges) {
        n += r.span_size();
    }
    return n;
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
inline void SparseRange<T>::split_and_reverse(size_t expected_range_cnt, size_t chunk_size) {
    if (size() < expected_range_cnt && span_size() > std::max(expected_range_cnt, chunk_size)) {
        size_t expected_size_each_range = 0;
        for (size_t i = 0; i < size(); ++i) {
            expected_size_each_range += _ranges[i].span_size();
        }

        expected_size_each_range /= expected_range_cnt;
        expected_size_each_range = std::max<size_t>(expected_size_each_range, 1);

        std::vector<Range<T>> new_ranges;
        for (auto range : _ranges) {
            while (range.span_size() > expected_size_each_range) {
                new_ranges.emplace_back(range.begin(), range.begin() + expected_size_each_range);
                range = Range<T>(range.begin() + expected_size_each_range, range.end());
            }
            new_ranges.emplace_back(range);
        }
        std::swap(_ranges, new_ranges);
    }
    std::reverse(_ranges.begin(), _ranges.end());
    _is_sorted = false;
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
inline Range<T> SparseRangeIterator<T>::next(T size) {
    const std::vector<Range<T>>& ranges = _range->_ranges;
    const Range<T>& range = ranges[_index];
    size = std::min<T>(size, range.end() - _next_rowid);
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
inline void SparseRangeIterator<T>::next_range(T size, SparseRange<T>* range) {
    while (size > 0 && has_more()) {
        Range r = next(size);
        range->add(r);
        size -= r.span_size();
        if (!_range->is_sorted()) {
            break;
        }
    }
}

template <typename T>
inline SparseRangeIterator<T> SparseRangeIterator<T>::intersection(const SparseRange<T>& rhs,
                                                                   SparseRange<T>* result) const {
    DCHECK(std::is_sorted(rhs._ranges.begin(), rhs._ranges.end(),
                          [](const auto& l, const auto& r) { return l.begin() < r.begin(); }));
    if (!has_more()) {
        return SparseRangeIterator<T>(result);
    }

    bool is_sorted = _range->is_sorted();
    auto ranges = std::vector<Range<T>>(_range->_ranges.begin() + _index, _range->_ranges.end());
    ranges[0] = Range<T>(_next_rowid, ranges[0].end());
    if (!is_sorted) {
        std::reverse(ranges.begin(), ranges.end());
    }
    DCHECK(std::is_sorted(ranges.begin(), ranges.end(),
                          [](const auto& l, const auto& r) { return l.begin() < r.begin(); }));

    for (size_t i = 0; i < ranges.size(); ++i) {
        const auto& r1 = ranges[i];
        for (const auto& r2 : rhs._ranges) {
            if (r1.end() < r2.begin()) {
                break;
            }
            if (r1.has_intersection(r2)) {
                result->_add_uncheck(r1.intersection(r2));
            }
        }
    }
    DCHECK(std::is_sorted(result->_ranges.begin(), result->_ranges.end(),
                          [](const auto& l, const auto& r) { return l.begin() < r.begin(); }));
    if (!is_sorted) {
        std::reverse(result->_ranges.begin(), result->_ranges.end());
    }

    SparseRangeIterator<T> res(result);
    return res;
}

template <typename T>
inline size_t SparseRangeIterator<T>::covered_ranges(size_t size) const {
    if (size == 0) {
        return 0;
    }
    const std::vector<Range<T>>& ranges = _range->_ranges;
    T end = std::min<T>(_next_rowid + size, ranges.back().end());
    size_t i = _index;
    for (; ranges[i].end() < end; i++) {
    }
    i += (end > ranges[i].begin());
    return i - _index;
}

template <typename T>
inline void SparseRangeIterator<T>::skip(T size) {
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
template class Range<ordinal_t>;
using RowIdRange = Range<rowid_t>;
using OridinalRange = Range<ordinal_t>;

template class SparseRange<>;
template class SparseRange<ordinal_t>;
using RowIdSparseRange = SparseRange<rowid_t>;
using OridinalSparseRange = SparseRange<ordinal_t>;

template class SparseRangeIterator<>;
template class SparseRangeIterator<ordinal_t>;
using RowIdSparseRangeIterator = SparseRangeIterator<rowid_t>;
using OrdinalSparseRangeIterator = SparseRangeIterator<ordinal_t>;

} // namespace starrocks
