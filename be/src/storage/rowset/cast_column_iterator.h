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

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "gutil/macros.h"
#include "storage/rowset/column_iterator_decorator.h"

namespace starrocks {

class Column;
class ColumnRef;
class Expr;
class ObjectPool;

class CastColumnIterator : public ColumnIteratorDecorator {
public:
    // REQUIRES:
    //  - |source_iter| cannot be NULL
    //  - |source_type| and |target_type| both are scalar type
    explicit CastColumnIterator(std::unique_ptr<ColumnIterator> source_iter, const TypeDescriptor& source_type,
                                const TypeDescriptor& target_type, bool nullable_source);

    ~CastColumnIterator() override;

    DISALLOW_COPY_AND_MOVE(CastColumnIterator);

    Status next_batch(size_t* n, Column* dst) override;

    Status next_batch(const SparseRange<>& range, Column* dst) override;

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

    // Disable bloom filter in CastColumnIterator
    bool has_original_bloom_filter_index() const override { return false; }
    bool has_ngram_bloom_filter_index() const override { return false; }
    Status get_row_ranges_by_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                          SparseRange<>* row_ranges) override {
        return Status::OK();
    }

private:
    void do_cast(Column* target);

    std::unique_ptr<ObjectPool> _obj_pool;
    // managed by |_obj_pool|
    Expr* _cast_expr;
    // Chunk for holding data read from the source column iterator
    Chunk _source_chunk;
};

} // namespace starrocks
