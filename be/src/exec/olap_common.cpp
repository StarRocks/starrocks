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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/olap_common.cpp

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

#include "exec/olap_common.h"

#include <algorithm>
#include <set>
#include <utility>
#include <vector>

#include "exec/olap_utils.h"
#include "storage/primitive/olap_tuple.h"
#include "storage/primitive/value_cast.h"

namespace starrocks {

Status OlapScanKeys::get_key_range(std::vector<std::unique_ptr<OlapScanRange>>* key_range) {
    key_range->clear();

    for (int i = 0; i < _begin_scan_keys.size(); ++i) {
        std::unique_ptr<OlapScanRange> range(new OlapScanRange());
        range->begin_scan_range = _begin_scan_keys[i];
        range->end_scan_range = _end_scan_keys[i];
        range->begin_include = _begin_include;
        range->end_include = _end_include;
        key_range->emplace_back(std::move(range));
    }

    return Status::OK();
}

template <class T>
Status OlapScanKeys::extend_scan_key(ColumnValueRange<T>& range, int32_t max_scan_key_num) {
    using namespace std;
    typedef typename set<T>::const_iterator const_iterator_type;

    // 1. clear ScanKey if some column range is empty
    if (range.is_empty_value_range()) {
        _begin_scan_keys.clear();
        _end_scan_keys.clear();
        return Status::Cancelled("empty column range");
    }

    // 2. stop extend ScanKey when it's already extend a range value
    if (_has_range_value) {
        return Status::Cancelled("already extend a range value");
    }

    if (range.is_fixed_value_range() && range.fixed_value_operator() == FILTER_NOT_IN) {
        return Status::Cancelled("stop extend scan key due to not in operator");
    }

    // if a column doesn't have any predicate, we will try to convert the range to fixed values
    // for this case, we need to add null value to fixed values
    bool has_converted = false;
    if (range.is_fixed_value_range()) {
        const size_t mul = std::max<size_t>(1, _begin_scan_keys.size());
        if (range.get_fixed_value_size() > max_scan_key_num / mul) {
            if (range.is_range_value_convertible()) {
                range.convert_to_range_value();
            } else {
                return Status::Cancelled("too many fixed values");
            }
        }
    } else if (range.is_fixed_value_convertible() && _is_convertible) {
        const size_t mul = std::max<size_t>(1, _begin_scan_keys.size());
        if (range.get_convertible_fixed_value_size() <= max_scan_key_num / mul) {
            if (range.is_low_value_mininum() && range.is_high_value_maximum()) {
                has_converted = true;
            }

            range.convert_to_fixed_value();
        }
    }

    // 3.1 extend ScanKey with FixedValueRange
    if (range.is_fixed_value_range()) {
        // 3.1.1 construct num of fixed value ScanKey (begin_key == end_key)
        if (_begin_scan_keys.empty()) {
            const auto& fixed_value_set = range.get_fixed_value_set();
            auto iter = fixed_value_set.begin();

            for (; iter != fixed_value_set.end(); ++iter) {
                _begin_scan_keys.emplace_back();
                _begin_scan_keys.back().add_value(
                        cast_to_string(*iter, range.type(), range.precision(), range.scale()));
                _end_scan_keys.emplace_back();
                _end_scan_keys.back().add_value(cast_to_string(*iter, range.type(), range.precision(), range.scale()));
            }

            if (has_converted) {
                _begin_scan_keys.emplace_back();
                _begin_scan_keys.back().add_null();
                _end_scan_keys.emplace_back();
                _end_scan_keys.back().add_null();
            }
        } // 3.1.2 produces the Cartesian product of ScanKey and fixed_value
        else {
            const auto& fixed_value_set = range.get_fixed_value_set();
            int original_key_range_size = _begin_scan_keys.size();

            for (int i = 0; i < original_key_range_size; ++i) {
                OlapTuple start_base_key_range = _begin_scan_keys[i];
                OlapTuple end_base_key_range = _end_scan_keys[i];

                auto iter = fixed_value_set.begin();

                for (; iter != fixed_value_set.end(); ++iter) {
                    // alter the first ScanKey in original place
                    if (iter == fixed_value_set.begin()) {
                        _begin_scan_keys[i].add_value(
                                cast_to_string(*iter, range.type(), range.precision(), range.scale()));
                        _end_scan_keys[i].add_value(
                                cast_to_string(*iter, range.type(), range.precision(), range.scale()));
                    } // append follow ScanKey
                    else {
                        _begin_scan_keys.push_back(start_base_key_range);
                        _begin_scan_keys.back().add_value(
                                cast_to_string(*iter, range.type(), range.precision(), range.scale()));
                        _end_scan_keys.push_back(end_base_key_range);
                        _end_scan_keys.back().add_value(
                                cast_to_string(*iter, range.type(), range.precision(), range.scale()));
                    }
                }

                if (has_converted) {
                    _begin_scan_keys.push_back(start_base_key_range);
                    _begin_scan_keys.back().add_null();
                    _end_scan_keys.push_back(end_base_key_range);
                    _end_scan_keys.back().add_null();
                }
            }
        }

        _begin_include = true;
        _end_include = true;
    } // Extend ScanKey with range value
    else {
        _has_range_value = true;

        if (_begin_scan_keys.empty()) {
            _begin_scan_keys.emplace_back();
            _begin_scan_keys.back().add_value(
                    cast_to_string(range.get_range_min_value(), range.type(), range.precision(), range.scale()),
                    range.is_low_value_mininum());
            _end_scan_keys.emplace_back();
            _end_scan_keys.back().add_value(
                    cast_to_string(range.get_range_max_value(), range.type(), range.precision(), range.scale()));
        } else {
            for (auto& _begin_scan_key : _begin_scan_keys) {
                _begin_scan_key.add_value(
                        cast_to_string(range.get_range_min_value(), range.type(), range.precision(), range.scale()),
                        range.is_low_value_mininum());
            }

            for (auto& _end_scan_key : _end_scan_keys) {
                _end_scan_key.add_value(
                        cast_to_string(range.get_range_max_value(), range.type(), range.precision(), range.scale()));
            }
        }

        _begin_include = range.is_begin_include();
        _end_include = range.is_end_include();
    }

    return Status::OK();
}

#define InsitializeColumnValueRange(T) \
    template Status OlapScanKeys::extend_scan_key<T>(ColumnValueRange<T> & range, int32_t max_scan_key_num);

InsitializeColumnValueRange(int8_t);
InsitializeColumnValueRange(uint8_t);
InsitializeColumnValueRange(int16_t);
InsitializeColumnValueRange(int32_t);
InsitializeColumnValueRange(int64_t);
InsitializeColumnValueRange(__int128);
InsitializeColumnValueRange(int256_t);
InsitializeColumnValueRange(Slice);
InsitializeColumnValueRange(DecimalV2Value);
InsitializeColumnValueRange(bool);
InsitializeColumnValueRange(DateValue);
InsitializeColumnValueRange(TimestampValue);

#undef InsitializeColumnValueRange

} // namespace starrocks

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
