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

#include "column_reader.h"
#include "common/status.h"
#include "io/shared_buffered_input_stream.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/range.h"
#include "storage/rowset/common.h"
#include "util/runtime_profile.h"

namespace starrocks {

class CondColumn;

class Column;
class ColumnAccessPath;
class ColumnPredicate;

class ColumnReader;
class RandomAccessFile;

struct ColumnIteratorOptions {
    //RandomAccessFile* read_file = nullptr;
    io::SeekableInputStream* read_file = nullptr;
    bool is_io_coalesce = false;
    // reader statistics
    OlapReaderStatistics* stats = nullptr;
    bool use_page_cache = false;
    LakeIOOptions lake_io_opts{.fill_data_cache = true};

    // check whether column pages are all dictionary encoding.
    bool check_dict_encoding = false;

    void sanity_check() const {
        CHECK_NOTNULL(read_file);
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
    [[nodiscard]] virtual Status seek_to_first() = 0;

    // Seek to the given ordinal entry in the column.
    // Entry 0 is the first entry written to the column.
    // If provided seek point is past the end of the file,
    // then returns false.
    [[nodiscard]] virtual Status seek_to_ordinal(ordinal_t ord) = 0;

    [[nodiscard]] virtual Status next_batch(size_t* n, Column* dst) = 0;

    [[nodiscard]] virtual Status next_batch(const SparseRange<>& range, Column* dst) {
        return Status::NotSupported("ColumnIterator Not Support batch read");
    }

    Status convert_sparse_range_to_io_range(const SparseRange<>& range) {
        if (auto sharedBufferStream = dynamic_cast<io::SharedBufferedInputStream*>(_opts.read_file);
            sharedBufferStream == nullptr) {
            return Status::OK();
        }

        auto reader = get_column_reader();
        if (reader == nullptr) {
            // should't happen
            LOG(INFO) << "column reader nullptr, filename: " << _opts.read_file->filename();
            return Status::OK();
        }

        std::vector<io::SharedBufferedInputStream::IORange> result;
        std::vector<std::pair<int, int>> page_index;
        int prev_page_index = -1;
        for (auto index = 0; index < range.size(); index++) {
            auto row_start = range[index].begin();
            auto row_end = range[index].end() - 1;
            OrdinalPageIndexIterator iter_start;
            OrdinalPageIndexIterator iter_end;
            RETURN_IF_ERROR(reader->seek_at_or_before(row_start, &iter_start));
            RETURN_IF_ERROR(reader->seek_at_or_before(row_end, &iter_end));

            if (prev_page_index == iter_start.page_index()) {
                // merge page index
                page_index.back().second = iter_end.page_index();
            } else {
                page_index.emplace_back(std::make_pair(iter_start.page_index(), iter_end.page_index()));
            }

            prev_page_index = iter_end.page_index();
        }

        for (auto pair : page_index) {
            OrdinalPageIndexIterator iter_start;
            OrdinalPageIndexIterator iter_end;
            RETURN_IF_ERROR(reader->seek_by_page_index(pair.first, &iter_start));
            RETURN_IF_ERROR(reader->seek_by_page_index(pair.second, &iter_end));
            auto offset = iter_start.page().offset;
            auto size = iter_end.page().offset - offset + iter_end.page().size;
            io::SharedBufferedInputStream::IORange io_range(offset, size);
            result.emplace_back(io_range);
        }

        return dynamic_cast<io::SharedBufferedInputStream*>(_opts.read_file)->set_io_ranges(result);
    }

    virtual ordinal_t get_current_ordinal() const = 0;

    /// for vectorized engine
    [[nodiscard]] virtual Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                            const ColumnPredicate* del_predicate,
                                                            SparseRange<>* row_ranges) = 0;

    [[nodiscard]] virtual Status get_row_ranges_by_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                                SparseRange<>* row_ranges) {
        return Status::OK();
    }

    // return true iff all data pages of this column are encoded as dictionary encoding.
    // NOTE: the ColumnIterator must have been initialized with `check_dict_encoding`,
    // otherwise this method will always return false.
    virtual bool all_page_dict_encoded() const { return false; }

    // if all data page of this colum are encoded as dictionary encoding.
    // return all dictionary words that store in dict page
    [[nodiscard]] virtual Status fetch_all_dict_words(std::vector<Slice>* words) const {
        return Status::NotSupported("Not Support dict.");
    }

    // only work when all_page_dict_encoded was true.
    // used to acquire load local dict
    virtual int dict_size() { return 0; }

    // return a non-negative dictionary code of |word| if it exist in this segment file,
    // otherwise -1 is returned.
    // NOTE: this method can be invoked only if `all_page_dict_encoded` returns true.
    virtual int dict_lookup(const Slice& word) { return -1; }

    // like `next_batch` but instead of return a batch of column values, this method returns a
    // batch of dictionary codes for dictionary encoded values.
    // this method can be invoked only if `all_page_dict_encoded` returns true.
    // type of |dst| must be `FixedLengthColumn<int32_t>` or `NullableColumn(FixedLengthColumn<int32_t>)`.
    [[nodiscard]] virtual Status next_dict_codes(size_t* n, Column* dst) { return Status::NotSupported(""); }

    [[nodiscard]] virtual Status next_dict_codes(const SparseRange<>& range, Column* dst) {
        return Status::NotSupported("");
    }

    // given a list of dictionary codes, fill |dst| column with the decoded values.
    // |codes| pointer to the array of dictionary codes.
    // |size| size of dictionary code array.
    // |words| column used to save the columns values, by append into it.
    [[nodiscard]] virtual Status decode_dict_codes(const int32_t* codes, size_t size, Column* words) {
        return Status::NotSupported("");
    }

    // Return the current array element position.
    virtual int64_t element_ordinal() const { return -1; }

    // This function used for array column.
    // Array column is made of offset and element.
    // This function seek to specified ordinal for offset column.
    // As well, calculate the element ordinal for element column.
    [[nodiscard]] virtual Status seek_to_ordinal_and_calc_element_ordinal(ordinal_t ord) {
        return Status::NotSupported("seek_to_ordinal_and_calc_element_ordinal");
    }

    // same as `decode_dict_codes(const int32_t*, size_t, Column*)` but extract
    // dictionary codes from the column |codes|.
    // |codes| must be of type `FixedLengthColumn<int32_t>` or `NullableColumn<FixedLengthColumn<int32_t>`
    // and assume no `null` value in |codes|.
    [[nodiscard]] virtual Status decode_dict_codes(const Column& codes, Column* words);

    // given a list of ordinals, fetch corresponding values.
    // |ordinals| must be ascending sorted.
    [[nodiscard]] virtual Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
        return Status::NotSupported("");
    }

    [[nodiscard]] Status fetch_values_by_rowid(const Column& rowids, Column* values);

    [[nodiscard]] virtual Status fetch_dict_codes_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
        return Status::NotSupported("");
    }

    [[nodiscard]] Status fetch_dict_codes_by_rowid(const Column& rowids, Column* values);

    // for Struct type (Struct)
    [[nodiscard]] virtual Status next_batch(size_t* n, Column* dst, ColumnAccessPath* path) {
        return next_batch(n, dst);
    }

    [[nodiscard]] virtual Status next_batch(const SparseRange<>& range, Column* dst, ColumnAccessPath* path) {
        return next_batch(range, dst);
    }

    [[nodiscard]] virtual Status fetch_subfield_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
        return Status::OK();
    }

protected:
    ColumnIteratorOptions _opts;
    virtual ColumnReader* get_column_reader() { return nullptr; };
};

} // namespace starrocks
