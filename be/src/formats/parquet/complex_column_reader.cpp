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

#include "formats/parquet/complex_column_reader.h"

#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "exprs/literal.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/schema.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "storage/column_expr_predicate.h"

namespace starrocks::parquet {

template <typename TOffset, typename TIsNull>
static void def_rep_to_offset(const LevelInfo& level_info, const level_t* def_levels, const level_t* rep_levels,
                              size_t num_levels, TOffset* offsets, TIsNull* is_nulls, size_t* num_offsets,
                              bool* has_null) {
    size_t offset_pos = 0;
    for (int i = 0; i < num_levels; ++i) {
        // when def_level is less than immediate_repeated_ancestor_def_level, it means that level
        // will affect its ancestor.
        // when rep_level is greater than max_rep_level, this means that level affects its
        // descendants.
        // So we can skip this levels
        if (def_levels[i] < level_info.immediate_repeated_ancestor_def_level ||
            rep_levels[i] > level_info.max_rep_level) {
            continue;
        }
        if (rep_levels[i] == level_info.max_rep_level) {
            offsets[offset_pos]++;
            continue;
        }

        // Start for a new row
        offset_pos++;
        offsets[offset_pos] = offsets[offset_pos - 1];
        if (def_levels[i] >= level_info.max_def_level) {
            offsets[offset_pos]++;
        }

        // when def_level equals with max_def_level, this is a non null element or a required element
        // when def_level equals with (max_def_level - 1), this indicates an empty array
        // when def_level less than (max_def_level - 1) it means this array is null
        if (def_levels[i] >= level_info.max_def_level - 1) {
            is_nulls[offset_pos - 1] = 0;
        } else {
            is_nulls[offset_pos - 1] = 1;
            *has_null = true;
        }
    }
    *num_offsets = offset_pos;
}

Status ListColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    NullableColumn* nullable_column = nullptr;
    ArrayColumn* array_column = nullptr;
    if (dst->is_nullable()) {
        nullable_column = down_cast<NullableColumn*>(dst.get());
        DCHECK(nullable_column->mutable_data_column()->is_array());
        array_column = down_cast<ArrayColumn*>(nullable_column->mutable_data_column());
    } else {
        DCHECK(dst->is_array());
        DCHECK(!get_column_parquet_field()->is_nullable);
        array_column = down_cast<ArrayColumn*>(dst.get());
    }
    auto& child_column = array_column->elements_column();
    RETURN_IF_ERROR(_element_reader->read_range(range, filter, child_column));

    level_t* def_levels = nullptr;
    level_t* rep_levels = nullptr;
    size_t num_levels = 0;
    _element_reader->get_levels(&def_levels, &rep_levels, &num_levels);

    auto& offsets = array_column->offsets_column()->get_data();
    offsets.resize(num_levels + 1);
    NullColumn null_column(num_levels);
    auto& is_nulls = null_column.get_data();
    size_t num_offsets = 0;
    bool has_null = false;
    def_rep_to_offset(get_column_parquet_field()->level_info, def_levels, rep_levels, num_levels, &offsets[0],
                      &is_nulls[0], &num_offsets, &has_null);
    offsets.resize(num_offsets + 1);
    is_nulls.resize(num_offsets);

    if (dst->is_nullable()) {
        DCHECK(nullable_column != nullptr);
        nullable_column->mutable_null_column()->swap_column(null_column);
        nullable_column->set_has_null(has_null);
    }

    return Status::OK();
}

Status ListColumnReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    ArrayColumn* array_column_src = nullptr;
    ArrayColumn* array_column_dst = nullptr;
    if (src->is_nullable()) {
        NullableColumn* nullable_column_src = down_cast<NullableColumn*>(src.get());
        DCHECK(nullable_column_src->mutable_data_column()->is_array());
        array_column_src = down_cast<ArrayColumn*>(nullable_column_src->mutable_data_column());
        NullableColumn* nullable_column_dst = down_cast<NullableColumn*>(dst.get());
        DCHECK(nullable_column_dst->mutable_data_column()->is_array());
        array_column_dst = down_cast<ArrayColumn*>(nullable_column_dst->mutable_data_column());
        nullable_column_dst->swap_null_column(*nullable_column_src);
    } else {
        DCHECK(src->is_array());
        DCHECK(dst->is_array());
        DCHECK(!get_column_parquet_field()->is_nullable);
        array_column_src = down_cast<ArrayColumn*>(src.get());
        array_column_dst = down_cast<ArrayColumn*>(dst.get());
    }
    array_column_dst->offsets_column()->swap_column(*(array_column_src->offsets_column()));
    RETURN_IF_ERROR(
            _element_reader->fill_dst_column(array_column_dst->elements_column(), array_column_src->elements_column()));
    return Status::OK();
}

Status MapColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    NullableColumn* nullable_column = nullptr;
    MapColumn* map_column = nullptr;
    if (dst->is_nullable()) {
        nullable_column = down_cast<NullableColumn*>(dst.get());
        DCHECK(nullable_column->mutable_data_column()->is_map());
        map_column = down_cast<MapColumn*>(nullable_column->mutable_data_column());
    } else {
        DCHECK(dst->is_map());
        DCHECK(!get_column_parquet_field()->is_nullable);
        map_column = down_cast<MapColumn*>(dst.get());
    }
    auto& key_column = map_column->keys_column();
    auto& value_column = map_column->values_column();
    if (_key_reader != nullptr) {
        RETURN_IF_ERROR(_key_reader->read_range(range, filter, key_column));
    }

    if (_value_reader != nullptr) {
        RETURN_IF_ERROR(_value_reader->read_range(range, filter, value_column));
    }

    // if neither key_reader not value_reader is nullptr , check the value_column size is the same with key_column
    DCHECK((_key_reader == nullptr) || (_value_reader == nullptr) || (value_column->size() == key_column->size()));

    level_t* def_levels = nullptr;
    level_t* rep_levels = nullptr;
    size_t num_levels = 0;

    if (_key_reader != nullptr) {
        _key_reader->get_levels(&def_levels, &rep_levels, &num_levels);
    } else if (_value_reader != nullptr) {
        _value_reader->get_levels(&def_levels, &rep_levels, &num_levels);
    } else {
        DCHECK(false) << "Unreachable!";
    }

    auto& offsets = map_column->offsets_column()->get_data();
    offsets.resize(num_levels + 1);
    NullColumn null_column(num_levels);
    auto& is_nulls = null_column.get_data();
    size_t num_offsets = 0;
    bool has_null = false;

    // ParquetFiled Map -> Map<Struct<key,value>>
    def_rep_to_offset(get_column_parquet_field()->level_info, def_levels, rep_levels, num_levels, &offsets[0],
                      &is_nulls[0], &num_offsets, &has_null);
    offsets.resize(num_offsets + 1);
    is_nulls.resize(num_offsets);

    // fill with default
    if (_key_reader == nullptr) {
        key_column->append_default(offsets.back());
    }
    if (_value_reader == nullptr) {
        value_column->append_default(offsets.back());
    }

    if (dst->is_nullable()) {
        DCHECK(nullable_column != nullptr);
        nullable_column->mutable_null_column()->swap_column(null_column);
        nullable_column->set_has_null(has_null);
    }

    return Status::OK();
}

Status StructColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    NullableColumn* nullable_column = nullptr;
    StructColumn* struct_column = nullptr;
    if (dst->is_nullable()) {
        nullable_column = down_cast<NullableColumn*>(dst.get());
        DCHECK(nullable_column->mutable_data_column()->is_struct());
        struct_column = down_cast<StructColumn*>(nullable_column->mutable_data_column());
    } else {
        DCHECK(dst->is_struct());
        DCHECK(!get_column_parquet_field()->is_nullable);
        struct_column = down_cast<StructColumn*>(dst.get());
    }

    const auto& field_names = struct_column->field_names();

    DCHECK_EQ(field_names.size(), _child_readers.size());

    // Fill data for subfield column reader
    size_t real_read = 0;
    bool first_read = true;
    for (size_t i = 0; i < field_names.size(); i++) {
        const auto& field_name = field_names[i];
        if (LIKELY(_child_readers.find(field_name) != _child_readers.end())) {
            if (_child_readers[field_name] != nullptr) {
                auto& child_column = struct_column->field_column(field_name);
                RETURN_IF_ERROR(_child_readers[field_name]->read_range(range, filter, child_column));
                real_read = child_column->size();
                first_read = false;
            }
        } else {
            return Status::InternalError(strings::Substitute("there is no match subfield reader for $1", field_name));
        }
    }

    if (UNLIKELY(first_read)) {
        return Status::InternalError(strings::Substitute("All used subfield of struct type $1 is not exist",
                                                         get_column_parquet_field()->name));
    }

    for (size_t i = 0; i < field_names.size(); i++) {
        const auto& field_name = field_names[i];
        if (_child_readers[field_name] == nullptr) {
            Column* child_column = struct_column->field_column(field_name).get();
            child_column->append_default(real_read);
        }
    }

    if (dst->is_nullable()) {
        DCHECK(nullable_column != nullptr);
        size_t row_nums = struct_column->fields_column()[0]->size();
        NullColumn null_column(row_nums, 0);
        auto& is_nulls = null_column.get_data();
        bool has_null = false;
        _handle_null_rows(is_nulls.data(), &has_null, row_nums);

        nullable_column->mutable_null_column()->swap_column(null_column);
        nullable_column->set_has_null(has_null);
    }
    return Status::OK();
}

bool StructColumnReader::try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                                const std::vector<std::string>& sub_field_path, const size_t& layer) {
    if (sub_field_path.size() <= layer) {
        return false;
    }
    const std::string& sub_field = sub_field_path[layer];
    if (_child_readers.find(sub_field) == _child_readers.end()) {
        return false;
    }

    if (_child_readers[sub_field] == nullptr) {
        return false;
    }
    return _child_readers[sub_field]->try_to_use_dict_filter(ctx, is_decode_needed, slotId, sub_field_path, layer + 1);
}

Status StructColumnReader::filter_dict_column(ColumnPtr& column, Filter* filter,
                                              const std::vector<std::string>& sub_field_path, const size_t& layer) {
    const std::string& sub_field = sub_field_path[layer];
    StructColumn* struct_column = nullptr;
    if (column->is_nullable()) {
        NullableColumn* nullable_column = down_cast<NullableColumn*>(column.get());
        DCHECK(nullable_column->mutable_data_column()->is_struct());
        struct_column = down_cast<StructColumn*>(nullable_column->mutable_data_column());
    } else {
        DCHECK(column->is_struct());
        DCHECK(!get_column_parquet_field()->is_nullable);
        struct_column = down_cast<StructColumn*>(column.get());
    }
    return _child_readers[sub_field]->filter_dict_column(struct_column->field_column(sub_field), filter, sub_field_path,
                                                         layer + 1);
}

Status StructColumnReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    StructColumn* struct_column_src = nullptr;
    StructColumn* struct_column_dst = nullptr;
    if (src->is_nullable()) {
        NullableColumn* nullable_column_src = down_cast<NullableColumn*>(src.get());
        DCHECK(nullable_column_src->mutable_data_column()->is_struct());
        struct_column_src = down_cast<StructColumn*>(nullable_column_src->mutable_data_column());
        NullableColumn* nullable_column_dst = down_cast<NullableColumn*>(dst.get());
        DCHECK(nullable_column_dst->mutable_data_column()->is_struct());
        struct_column_dst = down_cast<StructColumn*>(nullable_column_dst->mutable_data_column());
        nullable_column_dst->swap_null_column(*nullable_column_src);
    } else {
        DCHECK(src->is_struct());
        DCHECK(dst->is_struct());
        DCHECK(!get_column_parquet_field()->is_nullable);
        struct_column_src = down_cast<StructColumn*>(src.get());
        struct_column_dst = down_cast<StructColumn*>(dst.get());
    }
    const auto& field_names = struct_column_dst->field_names();
    for (size_t i = 0; i < field_names.size(); i++) {
        const auto& field_name = field_names[i];
        if (LIKELY(_child_readers.find(field_name) != _child_readers.end())) {
            if (_child_readers[field_name] == nullptr) {
                struct_column_dst->field_column(field_name)
                        ->swap_column(*(struct_column_src->field_column(field_name)));
            } else {
                RETURN_IF_ERROR(_child_readers[field_name]->fill_dst_column(
                        struct_column_dst->field_column(field_name), struct_column_src->field_column(field_name)));
            }
        } else {
            return Status::InternalError(strings::Substitute("there is no match subfield reader for $1", field_name));
        }
    }
    return Status::OK();
}

StatusOr<bool> StructColumnReader::row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                             CompoundNodeType pred_relation,
                                                             const uint64_t rg_first_row,
                                                             const uint64_t rg_num_rows) const {
    ObjectPool pool;

    auto is_filtered = [&](const ColumnPredicate* predicate) -> bool {
        std::vector<std::string> subfield{};
        auto res = _try_to_rewrite_subfield_expr(&pool, predicate, &subfield);
        // rewrite failed, always return true, select all
        RETURN_IF(!res.ok(), false);

        ColumnPredicate* rewritten_subfield_predicate = res.value();
        pool.add(rewritten_subfield_predicate);

        const ColumnReader* column_reader = get_child_column_reader(subfield);

        RETURN_IF(column_reader == nullptr, false);
        // make sure ColumnReader is scalar column
        RETURN_IF(column_reader->get_column_parquet_field()->type != ColumnType::SCALAR, false);

        auto ret = column_reader->row_group_zone_map_filter({rewritten_subfield_predicate}, pred_relation, rg_first_row,
                                                            rg_num_rows);
        // row_group_zone_map_filter failed, always return true, select all
        RETURN_IF(!ret.ok(), false);

        return ret.value();
    };

    std::vector<const ColumnPredicate*> rewritten_predicates;
    RETURN_IF_ERROR(_rewrite_column_expr_predicate(&pool, predicates, rewritten_predicates));
    if (rewritten_predicates.empty()) {
        return false;
    } else if (pred_relation == CompoundNodeType::AND) {
        return std::ranges::any_of(rewritten_predicates, [&](const auto* pred) { return is_filtered(pred); });
    } else {
        return std::ranges::all_of(rewritten_predicates, [&](const auto* pred) { return is_filtered(pred); });
    }
}

StatusOr<bool> StructColumnReader::page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                              SparseRange<uint64_t>* row_ranges,
                                                              CompoundNodeType pred_relation,
                                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) {
    DCHECK(row_ranges->empty());
    ObjectPool pool;

    auto handle_page_index = [&](const ColumnPredicate* predicate, SparseRange<uint64_t>* cur_row_ranges) -> bool {
        DCHECK(cur_row_ranges->empty());
        std::vector<std::string> subfield{};
        auto res = _try_to_rewrite_subfield_expr(&pool, predicate, &subfield);
        // rewrite failed, always return false, means no page index happened
        RETURN_IF(!res.ok(), false);

        ColumnPredicate* rewrite_subfield_predicate = res.value();
        pool.add(rewrite_subfield_predicate);

        ColumnReader* column_reader = get_child_column_reader(subfield);

        RETURN_IF(column_reader == nullptr, false);
        // make sure ColumnReader is scalar column
        RETURN_IF(column_reader->get_column_parquet_field()->type != ColumnType::SCALAR, false);

        auto ret = column_reader->page_index_zone_map_filter({rewrite_subfield_predicate}, cur_row_ranges,
                                                             pred_relation, rg_first_row, rg_num_rows);
        // page_index_zone_map_filter failed, always return false, no page index filter happened
        RETURN_IF(!res.ok(), false);

        return ret.value();
    };

    std::vector<const ColumnPredicate*> rewritten_predicates;
    RETURN_IF_ERROR(_rewrite_column_expr_predicate(&pool, predicates, rewritten_predicates));

    std::optional<SparseRange<uint64_t>> result_sparse_range = std::nullopt;

    for (const ColumnPredicate* predicate : rewritten_predicates) {
        SparseRange<uint64_t> tmp_row_ranges;

        if (!handle_page_index(predicate, &tmp_row_ranges)) {
            // no page index filter happened, means select all
            tmp_row_ranges.add({rg_first_row, rg_first_row + rg_num_rows});
        }

        if (pred_relation == CompoundNodeType::AND) {
            PredicateFilterEvaluatorUtils::merge_row_ranges<CompoundNodeType::AND>(result_sparse_range, tmp_row_ranges);
        } else {
            PredicateFilterEvaluatorUtils::merge_row_ranges<CompoundNodeType::OR>(result_sparse_range, tmp_row_ranges);
        }
    }

    if (!result_sparse_range.has_value()) {
        return false;
    }
    *row_ranges = std::move(result_sparse_range.value());
    return row_ranges->span_size() < rg_num_rows;
}

StatusOr<bool> StructColumnReader::row_group_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                          CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                                          const uint64_t rg_num_rows) const {
    ObjectPool pool;

    auto is_filtered = [&](const ColumnPredicate* predicate) -> bool {
        std::vector<std::string> subfield{};
        auto res = _try_to_rewrite_subfield_expr(&pool, predicate, &subfield);
        // rewrite failed, always return true, select all
        RETURN_IF(!res.ok(), false);

        ColumnPredicate* rewritten_subfield_predicate = res.value();
        pool.add(rewritten_subfield_predicate);
        const auto root =
                down_cast<ColumnExprPredicate*>(rewritten_subfield_predicate)->get_expr_ctxs().front()->root();
        if (root->op() != TExprOpcode::EQ) {
            return false;
        }
        if (!root->get_child(1)->is_literal()) {
            return false;
        }
        auto val = down_cast<const VectorizedLiteral*>(root->get_child(1))->value();
        if (!val || !val->is_constant()) {
            return false;
        }
        auto val_str = down_cast<const ConstColumn*>(val.get())->data_column()->debug_item(0);
        auto t = get_type_info(root->get_child(1)->type().type);
        ColumnPredicate* pred = nullptr;
        pred = new_column_eq_predicate(t, rewritten_subfield_predicate->column_id(), val_str);
        DCHECK(pred != nullptr);
        pool.add(pred);

        const ColumnReader* column_reader = get_child_column_reader(subfield);
        RETURN_IF(column_reader == nullptr, false);
        // make sure ColumnReader is scalar column
        RETURN_IF(column_reader->get_column_parquet_field()->type != ColumnType::SCALAR, false);
        auto ret = column_reader->row_group_bloom_filter({pred}, pred_relation, rg_first_row, rg_num_rows);
        return ret.value_or(false);
    };
    if (predicates.empty()) {
        return false;
    } else if (pred_relation == CompoundNodeType::AND) {
        return std::ranges::any_of(predicates, [&](const auto* pred) { return is_filtered(pred); });
    } else {
        return std::ranges::all_of(predicates, [&](const auto* pred) { return is_filtered(pred); });
    }
}

ColumnReader* StructColumnReader::get_child_column_reader(const std::string& subfield) const {
    auto it = _child_readers.find(subfield);
    if (it == _child_readers.end()) {
        return nullptr;
    } else {
        return it->second.get();
    }
}

ColumnReader* StructColumnReader::get_child_column_reader(const std::vector<std::string>& subfields) const {
    ColumnReader* column_reader = nullptr;
    for (const std::string& subfield : subfields) {
        if (column_reader == nullptr) {
            column_reader = get_child_column_reader(subfield);
        } else {
            const auto* struct_column_reader = down_cast<StructColumnReader*>(column_reader);
            column_reader = struct_column_reader->get_child_column_reader(subfield);
        }

        RETURN_IF(column_reader == nullptr, nullptr);
    }
    return column_reader;
}

// Rewrite ColumnExprPredicate which contains subfield expr and put subfield path into subfield_output
// For example, WHERE col.a.b.c > 5, a.b.c is subfields, we will rewrite it to c > 5
StatusOr<ColumnPredicate*> StructColumnReader::_try_to_rewrite_subfield_expr(
        ObjectPool* pool, const ColumnPredicate* predicate, std::vector<std::string>* subfield_output) const {
    // make sure it's expr predicate
    if (!predicate->is_expr_predicate()) {
        return Status::InternalError("Predicate is not an expression predicate");
    }

    const ColumnExprPredicate* expr_predicate = down_cast<const ColumnExprPredicate*>(predicate);
    const std::vector<ExprContext*>& expr_contexts = expr_predicate->get_expr_ctxs();
    if (expr_contexts.size() != 1) {
        // just defense code, make sure each ColumnExprPredicate has only one ExprContext
        return Status::InternalError("ColumnExprPredicate should has one ExprContext");
    }

    ExprContext* expr_context = expr_contexts[0];
    // Get root_expr like:
    //       OP_CODE(=, <, >, ...)
    //       /                  \
    //   subfield            right expr
    const Expr* root_expr = expr_context->root();
    const std::vector<Expr*>& expr_children = root_expr->children();
    // check there must have two children, and the left one is SubfieldExpr
    if (expr_children.size() != 2 || expr_children[0]->node_type() != TExprNodeType::type::SUBFIELD_EXPR) {
        return Status::InternalError("Invalid pattern for predicate");
    }

    Expr* subfield_expr = expr_children[0];
    Expr* right_expr = expr_children[1];

    // check exprs are monotonic
    if (root_expr->op() != TExprOpcode::type::EQ && !root_expr->is_monotonic()) {
        return Status::InternalError("Predicate's expr is not monotonic");
    } else if (!subfield_expr->is_monotonic() || !right_expr->is_monotonic()) {
        return Status::InternalError("Predicate's expr is not monotonic");
    }

    std::vector<std::vector<std::string>> subfields{};
    int num_subfield = subfield_expr->get_subfields(&subfields);
    if (num_subfield != 1) {
        // must only exist one subfield
        return Status::InternalError("Should have only one subfield path");
    }

    subfield_output->insert(subfield_output->end(), subfields[0].begin(), subfields[0].end());

    // check subfield expr has only one child, and it's a SlotRef
    if (subfield_expr->children().size() != 1 && !subfield_expr->get_child(0)->is_slotref()) {
        return Status::InternalError("Invalid pattern for predicate");
    }

    // extract subfield expr's SlotRef, and copy it
    Expr* new_slot_expr = Expr::copy(pool, subfield_expr->get_child(0));
    Expr* new_right_expr = Expr::copy(pool, right_expr);
    Expr* new_root_expr = root_expr->clone(pool);
    new_root_expr->set_monotonic(true);
    new_root_expr->add_child(new_slot_expr);
    new_root_expr->add_child(new_right_expr);

    const auto rewritten_expr = std::make_unique<ExprContext>(new_root_expr);
    RETURN_IF_ERROR(rewritten_expr->prepare(expr_predicate->runtime_state()));
    RETURN_IF_ERROR(rewritten_expr->open(expr_predicate->runtime_state()));
    return ColumnExprPredicate::make_column_expr_predicate(
            get_type_info(subfield_expr->type().type, subfield_expr->type().precision, subfield_expr->type().scale),
            expr_predicate->column_id(), expr_predicate->runtime_state(), rewritten_expr.get(),
            expr_predicate->slot_desc());
}

// rewrite std::vector<const ColumnPredicate*> src_preds
// If we face EQ ColumnExprPredicates, we have to rewrite it to ColumnExprPredicates <= val AND ColumnExprPredicates >= val
// So we can apply ZoneMap to evaluate.
Status StructColumnReader::_rewrite_column_expr_predicate(ObjectPool* pool,
                                                          const std::vector<const ColumnPredicate*>& src_preds,
                                                          std::vector<const ColumnPredicate*>& dst_preds) const {
    DCHECK(pool != nullptr);

    for (const ColumnPredicate* src_pred : src_preds) {
        // it's not ColumnExprPredicate, don't need to rewrite it
        if (!src_pred->is_expr_predicate()) {
            dst_preds.emplace_back(src_pred);
            continue;
        }

        const ColumnExprPredicate* expr_predicate = down_cast<const ColumnExprPredicate*>(src_pred);
        std::vector<const ColumnExprPredicate*> output;
        RETURN_IF_ERROR(expr_predicate->try_to_rewrite_for_zone_map_filter(pool, &output));
        if (output.empty()) {
            // no rewrite happened, insert the original predicate
            dst_preds.emplace_back(src_pred);
        } else {
            // insert rewritten predicates
            dst_preds.insert(dst_preds.end(), output.begin(), output.end());
        }
    }
    return Status::OK();
}

void StructColumnReader::_handle_null_rows(uint8_t* is_nulls, bool* has_null, size_t num_rows) {
    level_t* def_levels = nullptr;
    level_t* rep_levels = nullptr;
    size_t num_levels = 0;
    (*_def_rep_level_child_reader)->get_levels(&def_levels, &rep_levels, &num_levels);

    if (def_levels == nullptr) {
        // If subfields are required, def_levels is nullptr
        *has_null = false;
        return;
    }

    LevelInfo level_info = get_column_parquet_field()->level_info;

    if (rep_levels != nullptr) {
        // It's a RepeatedStoredColumnReader
        size_t rows = 0;
        for (size_t i = 0; i < num_levels; i++) {
            if (def_levels[i] < level_info.immediate_repeated_ancestor_def_level ||
                rep_levels[i] > level_info.max_rep_level) {
                continue;
            }

            // Start for a new row
            if (def_levels[i] >= level_info.max_def_level) {
                is_nulls[rows] = 0;
            } else {
                is_nulls[rows] = 1;
                *has_null = true;
            }
            rows++;
        }
        DCHECK_EQ(num_rows, rows);
    } else {
        // For OptionalStoredColumnReader, num_levels is equal to num_rows
        DCHECK(num_rows == num_levels);
        for (size_t i = 0; i < num_levels; i++) {
            if (def_levels[i] >= level_info.max_def_level) {
                is_nulls[i] = 0;
            } else {
                is_nulls[i] = 1;
                *has_null = true;
            }
        }
    }
}

} // namespace starrocks::parquet