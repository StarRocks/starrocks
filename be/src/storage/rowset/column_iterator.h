// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/column_reader.h

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

#include "common/status.h"
#include "storage/olap_common.h"
#include "storage/rowset/common.h"

namespace starrocks {

class CondColumn;
class ColumnBlockView;

namespace vectorized {
class Column;
class ColumnPredicate;
class SparseRange;
} // namespace vectorized

namespace fs {
class ReadableBlock;
} // namespace fs

class ColumnReader;

struct ColumnIteratorOptions {
    fs::ReadableBlock* rblock = nullptr;
    // reader statistics
    OlapReaderStatistics* stats = nullptr;
    bool use_page_cache = false;

    // check whether column pages are all dictionary encoding.
    bool check_dict_encoding = false;

    void sanity_check() const {
        CHECK_NOTNULL(rblock);
        CHECK_NOTNULL(stats);
    }

    // used to indicate that if this column is nullable, this flag can help ColumnIterator do
    // some optimization.
    bool is_nullable = true;

    ReaderType reader_type = READER_QUERY;
    int chunk_size = DEFAULT_CHUNK_SIZE;
};

// Base iterator to read one column data
class ColumnIterator {
public:
    ColumnIterator() = default;
    virtual ~ColumnIterator() = default;

    virtual Status init(const ColumnIteratorOptions& opts) {
        _opts = opts;
        return Status::OK();
    }

    // Seek to the first entry in the column.
    virtual Status seek_to_first() = 0;

    // Seek to the given ordinal entry in the column.
    // Entry 0 is the first entry written to the column.
    // If provided seek point is past the end of the file,
    // then returns false.
    virtual Status seek_to_ordinal(ordinal_t ord) = 0;

    Status next_batch(size_t* n, ColumnBlockView* dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    // After one seek, we can call this function many times to read data
    // into ColumnBlockView. when read string type data, memory will allocated
    // from MemPool
    virtual Status next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) = 0;

    virtual Status next_batch(size_t* n, vectorized::Column* dst) = 0;

    virtual Status next_batch(const vectorized::SparseRange& range, vectorized::Column* dst) {
        return Status::NotSupported("ColumnIterator Not Support batch read");
    }

    virtual ordinal_t get_current_ordinal() const = 0;

    /// for vectorized engine
    virtual Status get_row_ranges_by_zone_map(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                              const vectorized::ColumnPredicate* del_predicate,
                                              vectorized::SparseRange* row_ranges) = 0;

    virtual Status get_row_ranges_by_bloom_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                                  vectorized::SparseRange* row_ranges) {
        return Status::OK();
    }

    // return true iff all data pages of this column are encoded as dictionary encoding.
    // NOTE: the ColumnIterator must have been initialized with `check_dict_encoding`,
    // otherwise this method will always return false.
    virtual bool all_page_dict_encoded() const { return false; }

    // if all data page of this colum are encoded as dictionary encoding.
    // return all dictionary words that store in dict page
    virtual Status fetch_all_dict_words(std::vector<Slice>* words) const {
        return Status::NotSupported("Not Support dict.");
    }

    // return a non-negative dictionary code of |word| if it exist in this segment file,
    // otherwise -1 is returned.
    // NOTE: this method can be invoked only if `all_page_dict_encoded` returns true.
    virtual int dict_lookup(const Slice& word) { return -1; }

    // like `next_batch` but instead of return a batch of column values, this method returns a
    // batch of dictionary codes for dictionary encoded values.
    // this method can be invoked only if `all_page_dict_encoded` returns true.
    // type of |dst| must be `FixedLengthColumn<int32_t>` or `NullableColumn(FixedLengthColumn<int32_t>)`.
    virtual Status next_dict_codes(size_t* n, vectorized::Column* dst) { return Status::NotSupported(""); }

    virtual Status next_dict_codes(const vectorized::SparseRange& range, vectorized::Column* dst) {
        return Status::NotSupported("");
    }

    // given a list of dictionary codes, fill |dst| column with the decoded values.
    // |codes| pointer to the array of dictionary codes.
    // |size| size of dictionary code array.
    // |words| column used to save the columns values, by append into it.
    virtual Status decode_dict_codes(const int32_t* codes, size_t size, vectorized::Column* words) {
        return Status::NotSupported("");
    }

    // Return the current array element position.
    virtual int64_t element_ordinal() const { return -1; }

    // This function used for array column.
    // Array column is made of offset and element.
    // This function seek to specified ordinal for offset column.
    // As well, calculate the element ordinal for element column.
    virtual Status seek_to_ordinal_and_calc_element_ordinal(ordinal_t ord) {
        return Status::NotSupported("seek_to_ordinal_and_calc_element_ordinal");
    }

    // same as `decode_dict_codes(const int32_t*, size_t, vectorized::Column*)` but extract
    // dictionary codes from the column |codes|.
    // |codes| must be of type `FixedLengthColumn<int32_t>` or `NullableColumn<FixedLengthColumn<int32_t>`
    // and assume no `null` value in |codes|.
    virtual Status decode_dict_codes(const vectorized::Column& codes, vectorized::Column* words);

    // given a list of ordinals, fetch corresponding values.
    // |ordinals| must be ascending sorted.
    virtual Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) {
        return Status::NotSupported("");
    }

    Status fetch_values_by_rowid(const vectorized::Column& rowids, vectorized::Column* values);

    virtual Status fetch_dict_codes_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) {
        return Status::NotSupported("");
    }

    Status fetch_dict_codes_by_rowid(const vectorized::Column& rowids, vectorized::Column* values);

protected:
    ColumnIteratorOptions _opts;
};

} // namespace starrocks
