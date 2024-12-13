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
<<<<<<< HEAD
=======
#include "column/vectorized_fwd.h"
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
#include "storage/rowset/common.h"
#include "util/logging.h"

namespace starrocks {

<<<<<<< HEAD
// Range represent a logical contiguous range of a segment file.
// Range contains a inclusive start row number and an exclusive end row number.
class Range {
    using rowid_t = starrocks::rowid_t;

public:
    Range() = default;
    Range(rowid_t begin, rowid_t end);
=======
// Range represents a logical contiguous range of a segment file.
// Range contains an inclusive start row number and an exclusive end row number.
template <typename T = rowid_t>
class Range {
public:
    Range() = default;
    Range(T begin, T end);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    // Enable copy/move ctor and assignment.
    Range(const Range&) = default;
    Range& operator=(const Range&) = default;
    Range(Range&&) = default;
    Range& operator=(Range&&) = default;

    // the id of start row, inclusive.
<<<<<<< HEAD
    rowid_t begin() const { return _begin; }

    // the id of end row, exclusive.
    rowid_t end() const { return _end; }
=======
    T begin() const { return _begin; }

    // the id of end row, exclusive.
    T end() const { return _end; }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    bool empty() const { return span_size() == 0; }

    // number of rows covered by this range.
<<<<<<< HEAD
    uint32_t span_size() const { return _end - _begin; }
=======
    T span_size() const { return _end - _begin; }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    // return a new range that represent the intersection of |this| and |r|.
    Range intersection(const Range& r) const;

<<<<<<< HEAD
    bool has_intersection(const Range& rhs) const { return !(_end <= rhs.begin() || rhs.end() <= _begin); }

    bool contains(rowid_t row) const { return row < _end; }
=======
    Range filter(const Filter* const filter) const;

    bool has_intersection(const Range& rhs) const { return !(_end <= rhs.begin() || rhs.end() <= _begin); }

    bool contains(T row) const { return row < _end; }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    std::string to_string() const;

private:
<<<<<<< HEAD
    rowid_t _begin{0};
    rowid_t _end{0};
};

inline Range::Range(rowid_t begin, rowid_t end) : _begin(begin), _end(end) {
=======
    T _begin{0};
    T _end{0};
};

template <typename T>
inline Range<T>::Range(T begin, T end) : _begin(begin), _end(end) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    if (_begin >= _end) {
        _begin = 0;
        _end = 0;
    }
}

<<<<<<< HEAD
inline Range Range::intersection(const Range& r) const {
=======
template <typename T>
inline Range<T> Range<T>::intersection(const Range& r) const {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    if (!has_intersection(r)) {
        return {};
    }
    return {std::max(_begin, r._begin), std::min(_end, r._end)};
}

<<<<<<< HEAD
inline std::string Range::to_string() const {
=======
template <typename T>
inline Range<T> Range<T>::filter(const Filter* const filter) const {
    DCHECK(span_size() == filter->size());
    int32_t start = filter->size();
    int32_t end = -1;
    for (int32_t i = 0; i < filter->size(); i++) {
        start = start > i && filter->data()[i] == 1 ? i : start;
        end = end < i && filter->data()[i] == 1 ? i : end;
    }
    return start <= end ? Range<T>(_begin + start, _begin + end + 1) : Range<T>(_begin, _begin);
}

template <typename T>
inline std::string Range<T>::to_string() const {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    std::stringstream ss;
    ss << "[" << _begin << "," << _end << ")";
    return ss.str();
}

<<<<<<< HEAD
inline std::ostream& operator<<(std::ostream& os, const Range& range) {
    return (os << range.to_string());
}

inline bool operator==(const Range& r1, const Range& r2) {
    return r1.begin() == r2.begin() && r1.end() == r2.end();
}

inline bool operator!=(const Range& r1, const Range& r2) {
    return !(r1 == r2);
}

class SparseRange;

// SparseRangeIterator used to travel a SparseRange.
class SparseRangeIterator {
    using rowid_t = starrocks::rowid_t;

public:
    SparseRangeIterator() = default;
    explicit SparseRangeIterator(const SparseRange* r);

    SparseRangeIterator(const SparseRangeIterator& iter) = default;

    rowid_t begin() const { return _next_rowid; }

    // Return true iff there are untraversed range, i.e, `next` will return a non-empty range.
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    bool has_more() const;

    // Return the next contiguous range contains at most |size| rows.
    // `has_more` must be checked before calling this method.
<<<<<<< HEAD
    Range next(size_t size);

    // Return the next discontiguous range contains at most |size| rows
    void next_range(size_t size, SparseRange* range);

    // rhs should be a ordered sparse range
    SparseRangeIterator intersection(const SparseRange& rhs, SparseRange* result) const;

    void set_range(SparseRange* range) { _range = range; }

    size_t covered_ranges(size_t size) const;

    size_t convert_to_bitmap(uint8_t* bitmap, size_t max_size) const;

    void skip(size_t size);

private:
    const SparseRange* _range{nullptr};
    size_t _index{0};
    rowid_t _next_rowid{0};
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
};

// SparseRange represent a set of non-intersected contiguous ranges, or, in other words, represent
// a single non-contiguous range.
<<<<<<< HEAD
class SparseRange {
    using rowid_t = starrocks::rowid_t;

public:
    SparseRange() = default;
    SparseRange(rowid_t begin, rowid_t end) { add(Range(begin, end)); }
    explicit SparseRange(const Range r) { add(r); }
    SparseRange(const std::initializer_list<Range>& ranges) { add(ranges); }
=======
template <typename T = rowid_t>
class SparseRange {
public:
    SparseRange() = default;
    SparseRange(T begin, T end) { add(Range<T>(begin, end)); }
    explicit SparseRange(const Range<T> r) { add(r); }
    SparseRange(const std::initializer_list<Range<T>>& ranges) { add(ranges); }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    // true iff there are no sub-range.
    bool empty() const;

    void clear() { _ranges.clear(); }

    // number of sub-ranges
    size_t size() const { return _ranges.size(); }

    // begin of the first sub-range.
<<<<<<< HEAD
    rowid_t begin() const { return _ranges[0].begin(); }

    // end of the last sub-range.
    rowid_t end() const { return _ranges.back().end(); }

    // this method will invalidate iterator.
    void add(const Range& r);

    // this method will invalidate iterator.
    void add(const std::initializer_list<Range>& ranges);

    // number of rows covered by this range. it's the sum of all the sub-ranges span size.
    uint32_t span_size() const;

    // only contains single row or empty
    bool is_single_row_or_empty() const;

    // return a new range that represent the intersection of |this| and |r|.
    SparseRange intersection(const SparseRange& rhs) const;

    SparseRangeIterator new_iterator() const;

    std::string to_string() const;

    void split_and_revese(size_t expected_range_cnt, size_t chunk_size);
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    bool is_sorted() const { return _is_sorted; }
    void set_sorted(bool normalized) { _is_sorted = normalized; }

<<<<<<< HEAD
    bool operator==(const SparseRange& rhs) const;
    bool operator!=(const SparseRange& rhs) const;

    const Range& operator[](size_t idx) const { return _ranges[idx]; }
=======
    bool operator==(const SparseRange<T>& rhs) const;
    bool operator!=(const SparseRange<T>& rhs) const;

    const Range<T>& operator[](size_t idx) const { return _ranges[idx]; }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    SparseRange operator&(const SparseRange& rhs) const { return intersection(rhs); }

    SparseRange operator|(const SparseRange& rhs) const;

    SparseRange& operator&=(const SparseRange& rhs);

    SparseRange& operator|=(const SparseRange& rhs);

private:
<<<<<<< HEAD
    friend class SparseRangeIterator;

    void _add_uncheck(const Range& r);

    std::vector<Range> _ranges;
    bool _is_sorted = true;
};
using SparseRangePtr = std::shared_ptr<SparseRange>;

inline void SparseRange::_add_uncheck(const Range& r) {
=======
    friend class SparseRangeIterator<T>;

    void _add_uncheck(const Range<T>& r);

    std::vector<Range<T>> _ranges;
    bool _is_sorted = true;
};
using SparseRangePtr = std::shared_ptr<SparseRange<>>;

template <typename T>
inline void SparseRange<T>::_add_uncheck(const Range<T>& r) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    if (!r.empty()) {
        _ranges.emplace_back(r);
    }
}

<<<<<<< HEAD
inline bool SparseRange::empty() const {
    return _ranges.empty();
}

inline void SparseRange::add(const std::initializer_list<Range>& ranges) {
=======
template <typename T>
inline bool SparseRange<T>::empty() const {
    return _ranges.empty();
}

template <typename T>
inline void SparseRange<T>::add(const std::initializer_list<Range<T>>& ranges) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    for (const auto& r : ranges) {
        add(r);
    }
}

<<<<<<< HEAD
inline void SparseRange::add(const Range& r) {
=======
template <typename T>
inline void SparseRange<T>::add(const Range<T>& r) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
    std::vector<Range> new_ranges;
=======
    std::vector<Range<T>> new_ranges;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    new_ranges.reserve(_ranges.size() + 1);
    size_t i = 0;
    // ranges on the left side of |r| and not contiguous with |r|.
    for (; i < _ranges.size() && _ranges[i].end() < r.begin(); i++) {
        new_ranges.emplace_back(_ranges[i]);
    }
<<<<<<< HEAD
    // ranges that has intersection with |r| or contiguous with |r|.
    if (i < _ranges.size() && _ranges[i].begin() <= r.end()) {
        rowid_t b = std::min(_ranges[i].begin(), r.begin());
        rowid_t e = std::max(_ranges[i].end(), r.end());
=======
    // ranges that have intersection with |r| or contiguous with |r|.
    if (i < _ranges.size() && _ranges[i].begin() <= r.end()) {
        T b = std::min(_ranges[i].begin(), r.begin());
        T e = std::max(_ranges[i].end(), r.end());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
inline uint32_t SparseRange::span_size() const {
    size_t n = 0;
=======
template <typename T>
inline T SparseRange<T>::span_size() const {
    T n = 0;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    for (const auto& r : _ranges) {
        n += r.span_size();
    }
    return n;
}

<<<<<<< HEAD
inline bool SparseRange::is_single_row_or_empty() const {
    return _ranges.empty() || (_ranges.size() == 1 && _ranges[0].span_size() == 1);
}

inline SparseRangeIterator SparseRange::new_iterator() const {
    return SparseRangeIterator(this);
}

inline std::string SparseRange::to_string() const {
=======
template <typename T>
inline SparseRangeIterator<T> SparseRange<T>::new_iterator() const {
    return SparseRangeIterator(this);
}

template <typename T>
inline std::string SparseRange<T>::to_string() const {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    std::stringstream ss;
    ss << "(";
    for (const auto& r : _ranges) {
        ss << r.to_string() << ((&r == &_ranges.back()) ? "" : ", ");
    }
    ss << ")";
    return ss.str();
}

<<<<<<< HEAD
inline SparseRange SparseRange::intersection(const SparseRange& rhs) const {
=======
template <typename T>
inline SparseRange<T> SparseRange<T>::intersection(const SparseRange<T>& rhs) const {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
inline void SparseRange::split_and_revese(size_t expected_range_cnt, size_t chunk_size) {
=======
template <typename T>
inline void SparseRange<T>::split_and_reverse(size_t expected_range_cnt, size_t chunk_size) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    if (size() < expected_range_cnt && span_size() > std::max(expected_range_cnt, chunk_size)) {
        size_t expected_size_each_range = 0;
        for (size_t i = 0; i < size(); ++i) {
            expected_size_each_range += _ranges[i].span_size();
        }

        expected_size_each_range /= expected_range_cnt;
        expected_size_each_range = std::max<size_t>(expected_size_each_range, 1);

<<<<<<< HEAD
        std::vector<Range> new_ranges;
        for (auto range : _ranges) {
            while (range.span_size() > expected_size_each_range) {
                new_ranges.emplace_back(range.begin(), range.begin() + expected_size_each_range);
                range = Range(range.begin() + expected_size_each_range, range.end());
=======
        std::vector<Range<T>> new_ranges;
        for (auto range : _ranges) {
            while (range.span_size() > expected_size_each_range) {
                new_ranges.emplace_back(range.begin(), range.begin() + expected_size_each_range);
                range = Range<T>(range.begin() + expected_size_each_range, range.end());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            }
            new_ranges.emplace_back(range);
        }
        std::swap(_ranges, new_ranges);
<<<<<<< HEAD
        _is_sorted = false;
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }
    std::reverse(_ranges.begin(), _ranges.end());
    _is_sorted = false;
}

<<<<<<< HEAD
inline SparseRange SparseRange::operator|(const SparseRange& rhs) const {
=======
template <typename T>
inline SparseRange<T> SparseRange<T>::operator|(const SparseRange<T>& rhs) const {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    SparseRange res = *this;
    res |= rhs;
    return res;
}

<<<<<<< HEAD
inline SparseRange& SparseRange::operator&=(const SparseRange& rhs) {
    SparseRange tmp = this->intersection(rhs);
=======
template <typename T>
inline SparseRange<T>& SparseRange<T>::operator&=(const SparseRange<T>& rhs) {
    SparseRange<T> tmp = this->intersection(rhs);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    *this = std::move(tmp);
    return *this;
}

<<<<<<< HEAD
inline SparseRange& SparseRange::operator|=(const SparseRange& rhs) {
=======
template <typename T>
inline SparseRange<T>& SparseRange<T>::operator|=(const SparseRange<T>& rhs) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    for (size_t i = 0; i < rhs.size(); i++) {
        add(rhs[i]);
    }
    return *this;
}

<<<<<<< HEAD
inline SparseRangeIterator::SparseRangeIterator(const SparseRange* r) : _range(r) {
=======
template <typename T>
inline SparseRangeIterator<T>::SparseRangeIterator(const SparseRange<T>* r) : _range(r) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    if (!_range->_ranges.empty()) {
        _next_rowid = _range->_ranges[0].begin();
    }
}

<<<<<<< HEAD
inline bool SparseRangeIterator::has_more() const {
    return _index < _range->_ranges.size();
}

inline Range SparseRangeIterator::next(size_t size) {
    const std::vector<Range>& ranges = _range->_ranges;
    const Range& range = ranges[_index];
    size = std::min<size_t>(size, range.end() - _next_rowid);
    Range ret(_next_rowid, _next_rowid + size);
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    _next_rowid += size;
    if (_next_rowid == range.end()) {
        ++_index;
        if (_index < ranges.size()) {
            _next_rowid = ranges[_index].begin();
        }
    }
    return ret;
}

<<<<<<< HEAD
inline void SparseRangeIterator::next_range(size_t size, SparseRange* range) {
=======
template <typename T>
inline void SparseRangeIterator<T>::next_range(T size, SparseRange<T>* range) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    while (size > 0 && has_more()) {
        Range r = next(size);
        range->add(r);
        size -= r.span_size();
        if (!_range->is_sorted()) {
            break;
        }
    }
}

<<<<<<< HEAD
inline SparseRangeIterator SparseRangeIterator::intersection(const SparseRange& rhs, SparseRange* result) const {
    DCHECK(std::is_sorted(rhs._ranges.begin(), rhs._ranges.end(),
                          [](const auto& l, const auto& r) { return l.begin() < r.begin(); }));
    if (!has_more()) {
        return SparseRangeIterator(result);
    }

    bool is_sorted = _range->is_sorted();
    auto ranges = std::vector<Range>(_range->_ranges.begin() + _index, _range->_ranges.end());
    ranges[0] = Range(_next_rowid, ranges[0].end());
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
    SparseRangeIterator res(result);
    return res;
}

inline size_t SparseRangeIterator::covered_ranges(size_t size) const {
    if (size == 0) {
        return 0;
    }
    const std::vector<Range>& ranges = _range->_ranges;
    rowid_t end = std::min<rowid_t>(_next_rowid + size, ranges.back().end());
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    size_t i = _index;
    for (; ranges[i].end() < end; i++) {
    }
    i += (end > ranges[i].begin());
    return i - _index;
}

<<<<<<< HEAD
inline void SparseRangeIterator::skip(size_t size) {
    _next_rowid += size;
    const std::vector<Range>& ranges = _range->_ranges;
=======
template <typename T>
inline void SparseRangeIterator<T>::skip(T size) {
    _next_rowid += size;
    const std::vector<Range<T>>& ranges = _range->_ranges;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    while (_index < ranges.size() && ranges[_index].end() <= _next_rowid) {
        _index++;
    }
    if (_index < ranges.size()) {
        _next_rowid = std::max(_next_rowid, ranges[_index].begin());
    }
}

<<<<<<< HEAD
inline size_t SparseRangeIterator::convert_to_bitmap(uint8_t* bitmap, size_t max_size) const {
    rowid_t curr_row = _next_rowid;
    size_t index = _index;
    const std::vector<Range>& ranges = _range->_ranges;
    max_size = std::min<size_t>(max_size, ranges.back().end() - _next_rowid);
    DCHECK(!has_more() || ranges[_index].contains(curr_row));
    for (size_t i = 0; i < max_size; i++) {
        rowid_t b = ranges[index].begin();
        rowid_t e = ranges[index].end();
        bitmap[i] = (curr_row - b) < (e - b);
        curr_row++;
        index += (curr_row == e);
    }
    return max_size;
}

inline bool SparseRange::operator==(const SparseRange& rhs) const {
    return _ranges == rhs._ranges;
}

inline bool SparseRange::operator!=(const SparseRange& rhs) const {
    return !(*this == rhs);
}

inline std::ostream& operator<<(std::ostream& os, const SparseRange& range) {
    return (os << range.to_string());
}

=======
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

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
} // namespace starrocks
