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

#include "storage/chunk_predicate.h"

#include "rowset/column_iterator.h"
#include "storage/column_or_predicate.h"
#include "storage/predicate_parser.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// ChunkPredicate
// ------------------------------------------------------------------------------------

size_t ChunkPredicate::num_columns() {
    std::unordered_set<ColumnId> columns;
    (void)for_each_column_pred([&columns](ColumnPredicatePtr& col_pred) {
        columns.emplace(col_pred->column_id());
        return Status::OK();
    });
    return columns.size();
}

// ------------------------------------------------------------------------------------
// ColumnChunkPredicate
// ------------------------------------------------------------------------------------

Status ColumnChunkPredicate::evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    return _pred->evaluate(chunk->get_column_by_id(_pred->column_id()).get(), selection, from, to);
}
Status ColumnChunkPredicate::evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    return _pred->evaluate_and(chunk->get_column_by_id(_pred->column_id()).get(), selection, from, to);
}
Status ColumnChunkPredicate::evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    return _pred->evaluate_or(chunk->get_column_by_id(_pred->column_id()).get(), selection, from, to);
}

Status ColumnChunkPredicate::zone_map_filter(ColumnIterators& column_iterators,
                                             const std::map<ColumnId, ColumnOrPredicate>& del_preds,
                                             SparseRange<>* dest_row_ranges) const {
    const auto cid = _pred->column_id();
    const auto iter = del_preds.find(cid);
    const ColumnPredicate* del_pred = iter != del_preds.end() ? &(iter->second) : nullptr;

    return column_iterators[cid]->get_row_ranges_by_zone_map({_pred.get()}, del_pred, dest_row_ranges);
}

bool ColumnChunkPredicate::can_pushdown(PredicateParser* parser) const {
    return parser->can_pushdown(_pred.get());
}

ChunkPredicatePtr ColumnChunkPredicate::extract_non_pushdownable(PredicateParser* parser) {
    if (can_pushdown(parser)) {
        return nullptr;
    }
    return std::make_unique<ColumnChunkPredicate>(std::move(_pred));
}

size_t ColumnChunkPredicate::size() const {
    return _pred != nullptr;
}

Status ColumnChunkPredicate::for_each_column_pred(const ColumnPredicateVisitor& visitor) {
    return visitor(_pred);
}

Status ColumnChunkPredicate::accept(ChunkPredicateVisitor2& visitor) {
    return visitor.visit_column_pred(this);
}

// ------------------------------------------------------------------------------------
// CompoundChunkPredicate
// ------------------------------------------------------------------------------------

bool CompoundChunkPredicate::can_pushdown(PredicateParser* parser) const {
    return std::all_of(_preds.begin(), _preds.end(),
                       [parser](const auto& pred) { return parser->can_pushdown(pred.get()); });
}

ChunkPredicatePtr CompoundChunkPredicate::extract_non_pushdownable(PredicateParser* parser) {
    std::vector<ChunkPredicatePtr> non_pushdownable;
    std::vector<ChunkPredicatePtr> pushdownable;
    for (auto& pred : _preds) {
        if (pred->can_pushdown(parser)) {
            pushdownable.push_back(std::move(pred));
        } else {
            non_pushdownable.push_back(std::move(pred));
        }
    }
    _preds = std::move(pushdownable);

    auto non_pushdownable_pred = std::make_unique<CompoundChunkPredicate>();
    for (auto& pred : non_pushdownable) {
        non_pushdownable_pred->add_child_predicate(std::move(pred));
    }
    return non_pushdownable_pred;
}

size_t CompoundChunkPredicate::size() const {
    return std::accumulate(_preds.begin(), _preds.end(), 0,
                           [](size_t acc, const auto& pred) { return acc + pred->size(); });
}

Status CompoundChunkPredicate::for_each_column_pred(const ColumnPredicateVisitor& visitor) {
    for (auto& pred : _preds) {
        RETURN_IF_ERROR(pred->for_each_column_pred(visitor));
    }
    return Status::OK();
}

// ------------------------------------------------------------------------------------
// AndChunkPredicate
// ------------------------------------------------------------------------------------

class AndChunkPredicate final : public CompoundChunkPredicate {
public:
    ~AndChunkPredicate() override = default;

    Status evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;
    Status evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;
    Status evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;

    Status zone_map_filter(ColumnIterators& column_iterators, const std::map<ColumnId, ColumnOrPredicate>& del_preds,
                           SparseRange<>* dest_row_ranges) const override;

   Status accept(ChunkPredicateVisitor2& visitor) override;

private:
    mutable std::vector<uint8_t> _or_selection_buffer;
};

Status AndChunkPredicate::evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    if (empty()) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_preds[0]->evaluate(chunk, selection, from, to));
    for (int i = 1; i < _preds.size(); i++) {
        RETURN_IF_ERROR(_preds[i]->evaluate_and(chunk, selection, from, to));
    }

    return Status::OK();
}
Status AndChunkPredicate::evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    for (auto& pred : _preds) {
        RETURN_IF_ERROR(pred->evaluate_and(chunk, selection, from, to));
    }
    return Status::OK();
}
Status AndChunkPredicate::evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    if (empty()) {
        return Status::OK();
    }

    if (UNLIKELY(_or_selection_buffer.size() < to)) {
        _or_selection_buffer.resize(to);
    }
    auto* or_selection = _or_selection_buffer.data();

    RETURN_IF_ERROR(evaluate(chunk, or_selection, from, to));
    for (int i = from; i < to; i++) {
        selection[i] |= or_selection[i];
    }

    return Status::OK();
}

Status AndChunkPredicate::zone_map_filter(ColumnIterators& column_iterators,
                                          const std::map<ColumnId, ColumnOrPredicate>& del_preds,
                                          SparseRange<>* dest_row_ranges) const {
    if (empty()) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_preds[0]->zone_map_filter(column_iterators, del_preds, dest_row_ranges));

    for (int i = 1; i < _preds.size(); i++) {
        SparseRange<> cur_range;
        RETURN_IF_ERROR(_preds[i]->zone_map_filter(column_iterators, del_preds, &cur_range));
        *dest_row_ranges &= cur_range;
    }

    return Status::OK();
}

Status AndChunkPredicate::accept(ChunkPredicateVisitor2& visitor) {
    return visitor.visit_and_pred(this);
}

// ------------------------------------------------------------------------------------
// OrChunkPredicate
// ------------------------------------------------------------------------------------

class OrChunkPredicate final : public CompoundChunkPredicate {
public:
    ~OrChunkPredicate() override = default;

    Status evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;
    Status evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;
    Status evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;

    Status zone_map_filter(ColumnIterators& column_iterators, const std::map<ColumnId, ColumnOrPredicate>& del_preds,
                           SparseRange<>* dest_row_ranges) const override;

    Status accept(ChunkPredicateVisitor2& visitor) override;

private:
    mutable std::vector<uint8_t> _and_selection_buffer;
};

Status OrChunkPredicate::evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    if (empty()) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_preds[0]->evaluate(chunk, selection, from, to));
    for (int i = 1; i < _preds.size(); i++) {
        RETURN_IF_ERROR(_preds[i]->evaluate_or(chunk, selection, from, to));
    }

    return Status::OK();
}
Status OrChunkPredicate::evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    if (empty()) {
        return Status::OK();
    }

    if (UNLIKELY(_and_selection_buffer.size() < to)) {
        _and_selection_buffer.resize(to);
    }
    auto* and_selection = _and_selection_buffer.data();

    RETURN_IF_ERROR(evaluate(chunk, and_selection, from, to));
    for (int i = from; i < to; i++) {
        selection[i] &= and_selection[i];
    }

    return Status::OK();
}
Status OrChunkPredicate::evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    for (auto& pred : _preds) {
        RETURN_IF_ERROR(pred->evaluate_or(chunk, selection, from, to));
    }
    return Status::OK();
}

Status OrChunkPredicate::zone_map_filter(ColumnIterators& column_iterators,
                                         const std::map<ColumnId, ColumnOrPredicate>& del_preds,
                                         SparseRange<>* dest_row_ranges) const {
    for (auto& pred : _preds) {
        RETURN_IF_ERROR(pred->zone_map_filter(column_iterators, del_preds, dest_row_ranges));
    }
    return Status::OK();
}

Status OrChunkPredicate::accept(ChunkPredicateVisitor2& visitor) {
    return visitor.visit_or_pred(this);
}

ChunkPredicatePtr CompoundChunkPredicate::create_and() {
    return std::make_unique<AndChunkPredicate>();
}
ChunkPredicatePtr CompoundChunkPredicate::create_or() {
    return std::make_unique<OrChunkPredicate>();
}


// ------------------------------------------------------------------------------------
// ChunkPredicateVisitor
// ------------------------------------------------------------------------------------

Status ChunkPredicateVisitor2::visit_column_pred(ColumnChunkPredicate* pred) {
    return visit(pred);
}
Status ChunkPredicateVisitor2::visit_and_pred(CompoundChunkPredicate* pred) {
    return visit(pred);
}
Status ChunkPredicateVisitor2::visit_or_pred(CompoundChunkPredicate* pred) {
    return visit(pred);
}

} // namespace starrocks
