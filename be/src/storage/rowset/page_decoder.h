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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/page_decoder.h

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

#include "column/nullable_column.h"
#include "common/status.h" // for Status
#include "gen_cpp/segment.pb.h"
#include "storage/range.h"
#include "storage/rowset/page_handle_fwd.h"

namespace starrocks {
class ColumnPredicate;
class Column;
} // namespace starrocks

namespace starrocks {

// PageDecoder is used to decode page.
class PageDecoder {
public:
    PageDecoder() = default;

    virtual ~PageDecoder() = default;

    // Call this to do some preparation for decoder.
    // eg: parse data page header
    virtual Status init() = 0;

    // Seek the decoder to the given positional index of the page.
    // For example, seek_to_position_in_page(0) seeks to the first
    // stored entry.
    //
    // It is an error to call this with a value larger than Count().
    // Doing so has undefined results.
    virtual Status seek_to_position_in_page(uint32_t pos) = 0;

    // Seek the decoder to the given value in the page, or the
    // lowest value which is greater than the given value.
    //
    // If the decoder was able to locate an exact match, then
    // sets *exact_match to true. Otherwise sets *exact_match to
    // false, to indicate that the seeked value is _after_ the
    // requested value.
    //
    // If the given value is less than the lowest value in the page,
    // seeks to the start of the page. If it is higher than the highest
    // value in the page, then returns Status::NotFound
    //
    // This will only return valid results when the data page
    // consists of values in sorted order.
    virtual Status seek_at_or_after_value(const void* value, bool* exact_match) {
        return Status::NotSupported("seek_at_or_after_value"); // FIXME
    }

    virtual Status next_batch(size_t* n, Column* column) {
        return Status::NotSupported("vectorized not supported yet");
    }

    virtual Status next_batch(const SparseRange<>& range, Column* column) {
        return Status::NotSupported("PageDecoder Not Support");
    }

    // given a set of ranges in page, apply compound and predicates on it, and only return filtered data
    // since null data is separate from actually data page, we need pass the null data by caller if this is a nullable column
    // null_data is a uint8_t array where null_data[i] indicates whether the i-th row is null (1 for null, 0 for not null)
    // callee is responsible to handle null column and append null data into dst column if selected
    virtual Status next_batch_with_filter(Column* column, const SparseRange<>& range,
                                          const std::vector<const ColumnPredicate*>& compound_and_predicates,
                                          const uint8_t* null_data, uint8_t* selection, uint16_t* selected_idx) {
        return Status::NotSupported("PageDecoder Not Support next_batch_with_filter");
    }

    /**
     * rowids and count represent a rowId vector
     * read_by_rowids read selected rows in rowId vector
     *  and return row numbers it read by 'count'
     */
    virtual Status read_by_rowids(const ordinal_t first_ordinal_in_page, const rowid_t* rowids, size_t* count,
                                  Column* column) {
        return Status::NotSupported("PageDecoder Not Support");
    }

    virtual Status read_dict_codes_by_rowids(const ordinal_t first_ordinal_in_page, const rowid_t* rowids,
                                             size_t* count, Column* dst) {
        return Status::NotSupported("PageDecoder Doesn't Support read_dict_codes_by_rowids");
    }

    // Return the number of elements in this page.
    virtual uint32_t count() const = 0;

    // Return the position within the page of the currently seeked
    // entry (ie the entry that will next be returned by next_vector())
    virtual uint32_t current_index() const = 0;

    // Return the the encoding type of current page.
    // normally, the encoding type is fixed for all data pages of a single column, but
    // dictionary encoded column is an exception: it first use dictionary encoding as
    // the codec algorithm then switched to plain encoding when the dictionary page is full.
    virtual EncodingTypePB encoding_type() const = 0;

    virtual Status next_dict_codes(size_t* n, Column* dst) {
        return Status::NotSupported("next_dict_codes() not supported");
    }

    virtual Status next_dict_codes(const SparseRange<>& range, Column* dst) {
        return Status::NotSupported("next_dict_codes() not supported");
    }

    virtual const PageDecoder* dict_page_decoder() const { return nullptr; }

    PageDecoder(const PageDecoder&) = delete;
    const PageDecoder& operator=(const PageDecoder&) = delete;

    void set_page_handle(const std::shared_ptr<PageHandle>& page_handle) { _page_handle = page_handle; }

    virtual void reserve_col(size_t n, Column* column) { column->reserve(n); }

    virtual bool supports_read_by_rowids() const { return false; }

protected:
    std::shared_ptr<PageHandle> _page_handle;
};

} // namespace starrocks
