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

#include <memory>
#include <stack>
#include <vector>

#include "storage/column_predicate.h"

namespace starrocks {

class ColumnPredicate;
using ColumnPredicatePtr = std::unique_ptr<ColumnPredicate>;

class ChunkPredicate;
using ChunkPredicatePtr = std::unique_ptr<ChunkPredicate>;
class ColumnChunkPredicate;
class CompoundChunkPredicate;

class ColumnIterator;
using ColumnIterators = std::vector<std::unique_ptr<ColumnIterator>>;

class PredicateParser;
class ColumnOrPredicate;

class ChunkPredicateVisitor2 {
public:
    virtual ~ChunkPredicateVisitor2() = default;

    virtual Status visit(ChunkPredicate*) { return Status::OK(); }
    virtual Status visit_column_pred(ColumnChunkPredicate*);
    virtual Status visit_and_pred(CompoundChunkPredicate*);
    virtual Status visit_or_pred(CompoundChunkPredicate*);
};

class ChunkPredicate {
public:
    virtual ~ChunkPredicate() = default;

    Status evaluate(const Chunk* chunk, uint8_t* selection) const {
        return evaluate(chunk, selection, 0, chunk->num_rows());
    }
    Status evaluate_and(const Chunk* chunk, uint8_t* selection) const {
        return evaluate_and(chunk, selection, 0, chunk->num_rows());
    }
    Status evaluate_or(const Chunk* chunk, uint8_t* selection) const {
        return evaluate_or(chunk, selection, 0, chunk->num_rows());
    }

    virtual Status evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const = 0;
    virtual Status evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const = 0;
    virtual Status evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const = 0;

    virtual Status zone_map_filter(ColumnIterators& column_iterators,
                                   const std::map<ColumnId, ColumnOrPredicate>& del_preds,
                                   SparseRange<>* dest_row_ranges) const = 0;

    virtual bool can_pushdown(PredicateParser* parser) const = 0;
    virtual ChunkPredicatePtr extract_non_pushdownable(PredicateParser* parser) = 0;

    virtual size_t size() const = 0;
    bool empty() const { return size() == 0; }
    // TODO(lzh): should be const, but need a const version of for_each_column_pred.



    using ColumnPredicateVisitor = std::function<Status(ColumnPredicatePtr&)>;
    virtual Status for_each_column_pred(const ColumnPredicateVisitor& visitor) = 0;

    virtual Status accept(ChunkPredicateVisitor2& visitor) = 0;

    std::unordered_map<ColumnId, std::vector<const ColumnPredicate*>>& get_column_preds() const {
        if (!_cache_column_preds.has_value()) {
            auto& column_prds = _cache_column_preds.emplace();
            _collect_column_preds(column_prds, 0);
        }
        return _cache_column_preds.value();
    }

    std::map<ColumnId, std::vector<const ColumnPredicate*>>& get_all_column_preds();
    size_t num_columns();
    bool contains_column(ColumnId cid);

    // TODO(lzh): wa, this cannot be procted, because CompoundChunkPredicate call ChunkPredicate::_collect_column_preds
    virtual void _collect_column_preds(std::unordered_map<ColumnId, std::vector<const ColumnPredicate*>>& column_preds,
                                       int deep) const = 0;

protected:
    mutable std::optional<std::unordered_map<ColumnId, std::vector<const ColumnPredicate*>>> _cache_column_preds;
    mutable std::optional<std::map<ColumnId, std::vector<const ColumnPredicate*>>> _all_column_preds;
};

// TODO(lzh): use template
class ColumnChunkPredicate final : public ChunkPredicate {
public:
    explicit ColumnChunkPredicate(ColumnPredicatePtr pred) : _pred(std::move(pred)) {}
    ~ColumnChunkPredicate() override = default;

    Status evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;
    Status evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;
    Status evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;

    Status zone_map_filter(ColumnIterators& column_iterators, const std::map<ColumnId, ColumnOrPredicate>& del_preds,
                           SparseRange<>* dest_row_ranges) const override;

    bool can_pushdown(PredicateParser* parser) const override;
    ChunkPredicatePtr extract_non_pushdownable(PredicateParser* parser) override;

    size_t size() const override;

    Status for_each_column_pred(const ColumnPredicateVisitor& visitor) override;
    Status accept(ChunkPredicateVisitor2& visitor) override;

    ColumnPredicate* pred() const { return _pred.get(); }

    void _collect_column_preds(std::unordered_map<ColumnId, std::vector<const ColumnPredicate*>>& column_preds,int deep) const override;

protected:
private:
    ColumnPredicatePtr _pred;
};

class CompoundChunkPredicate : public ChunkPredicate {
public:
    ~CompoundChunkPredicate() override = default;

    void add_child_predicate(ChunkPredicatePtr child_pred) { _preds.emplace_back(std::move(child_pred)); }

    bool can_pushdown(PredicateParser* parser) const override;
    ChunkPredicatePtr extract_non_pushdownable(PredicateParser* parser) override;

    size_t size() const override;

    Status for_each_column_pred(const ColumnPredicateVisitor& visitor) override;

    std::vector<ChunkPredicatePtr>& preds() { return _preds; }

    void _collect_column_preds(std::unordered_map<ColumnId, std::vector<const ColumnPredicate*>>& column_preds,int deep) const override;

protected:
public:
    static ChunkPredicatePtr create_and();
    static ChunkPredicatePtr create_or();

protected:
    std::vector<ChunkPredicatePtr> _preds;
};

} // namespace starrocks
