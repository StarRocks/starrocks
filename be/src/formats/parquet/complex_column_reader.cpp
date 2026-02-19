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

#include "base/string/slice.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/variant_column.h"
#include "exprs/literal.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/schema.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "storage/column_expr_predicate.h"
#include "types/variant_value.h"
#include "util/variant_encoder.h"

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
        nullable_column = down_cast<NullableColumn*>(dst->as_mutable_raw_ptr());
        DCHECK(nullable_column->data_column_raw_ptr()->is_array());
        array_column = down_cast<ArrayColumn*>(nullable_column->data_column_raw_ptr());
    } else {
        DCHECK(dst->is_array());
        DCHECK(!get_column_parquet_field()->is_nullable);
        array_column = down_cast<ArrayColumn*>(dst->as_mutable_raw_ptr());
    }
    ColumnPtr& child_column = array_column->elements_column();
    RETURN_IF_ERROR(_element_reader->read_range(range, filter, child_column));

    level_t* def_levels = nullptr;
    level_t* rep_levels = nullptr;
    size_t num_levels = 0;
    _element_reader->get_levels(&def_levels, &rep_levels, &num_levels);

    auto& offsets = array_column->offsets_column_raw_ptr()->get_data();
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
        nullable_column->null_column_raw_ptr()->swap_column(null_column);
        nullable_column->set_has_null(has_null);
    }

    return Status::OK();
}

Status ListColumnReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src_in) {
    auto* src = src_in->as_mutable_raw_ptr();
    auto* dst_mut = dst->as_mutable_raw_ptr();
    ArrayColumn* array_column_src = nullptr;
    ArrayColumn* array_column_dst = nullptr;
    if (src->is_nullable()) {
        NullableColumn* nullable_column_src = down_cast<NullableColumn*>(src);
        DCHECK(nullable_column_src->data_column_raw_ptr()->is_array());
        array_column_src = down_cast<ArrayColumn*>(nullable_column_src->data_column_raw_ptr());
        NullableColumn* nullable_column_dst = down_cast<NullableColumn*>(dst_mut);
        DCHECK(nullable_column_dst->data_column_raw_ptr()->is_array());
        array_column_dst = down_cast<ArrayColumn*>(nullable_column_dst->data_column_raw_ptr());
        nullable_column_dst->swap_null_column(*nullable_column_src);
    } else {
        DCHECK(src->is_array());
        DCHECK(dst->is_array());
        DCHECK(!get_column_parquet_field()->is_nullable);
        array_column_src = down_cast<ArrayColumn*>(src);
        array_column_dst = down_cast<ArrayColumn*>(dst_mut);
    }
    auto* dst_offsets = array_column_dst->offsets_column_raw_ptr();
    auto* src_offsets = array_column_src->offsets_column_raw_ptr();
    dst_offsets->swap_column(*src_offsets);

    auto& dst_elements = array_column_dst->elements_column();
    auto& src_elements = array_column_src->elements_column();
    RETURN_IF_ERROR(_element_reader->fill_dst_column(dst_elements, src_elements));
    return Status::OK();
}

Status MapColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    NullableColumn* nullable_column = nullptr;
    MapColumn* map_column = nullptr;
    if (dst->is_nullable()) {
        nullable_column = down_cast<NullableColumn*>(dst->as_mutable_raw_ptr());
        DCHECK(nullable_column->data_column_raw_ptr()->is_map());
        map_column = down_cast<MapColumn*>(nullable_column->data_column_raw_ptr());
    } else {
        DCHECK(dst->is_map());
        DCHECK(!get_column_parquet_field()->is_nullable);
        map_column = down_cast<MapColumn*>(dst->as_mutable_raw_ptr());
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

    auto& offsets = map_column->offsets_column_raw_ptr()->get_data();
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
        key_column->as_mutable_raw_ptr()->append_default(offsets.back());
    }
    if (_value_reader == nullptr) {
        value_column->as_mutable_raw_ptr()->append_default(offsets.back());
    }

    if (dst->is_nullable()) {
        DCHECK(nullable_column != nullptr);
        nullable_column->null_column_raw_ptr()->swap_column(null_column);
        nullable_column->set_has_null(has_null);
    }

    return Status::OK();
}

Status StructColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    NullableColumn* nullable_column = nullptr;
    StructColumn* struct_column = nullptr;
    if (dst->is_nullable()) {
        nullable_column = down_cast<NullableColumn*>(dst->as_mutable_raw_ptr());
        DCHECK(nullable_column->data_column_raw_ptr()->is_struct());
        struct_column = down_cast<StructColumn*>(nullable_column->data_column_raw_ptr());
    } else {
        DCHECK(dst->is_struct());
        DCHECK(!get_column_parquet_field()->is_nullable);
        struct_column = down_cast<StructColumn*>(dst->as_mutable_raw_ptr());
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
                ASSIGN_OR_RETURN(auto& child_column, struct_column->field_column(field_name));
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
            ASSIGN_OR_RETURN(auto* child_column, struct_column->field_column_raw_ptr(field_name));

            child_column->append_default(real_read);
        }
    }

    if (dst->is_nullable()) {
        DCHECK(nullable_column != nullptr);
        size_t row_nums = struct_column->fields()[0]->size();
        NullColumn null_column(row_nums, 0);
        auto& is_nulls = null_column.get_data();
        bool has_null = false;
        _handle_null_rows(is_nulls.data(), &has_null, row_nums);

        nullable_column->null_column_raw_ptr()->swap_column(null_column);
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
    auto* column_mut = column->as_mutable_raw_ptr();
    StructColumn* struct_column = nullptr;
    if (column->is_nullable()) {
        NullableColumn* nullable_column = down_cast<NullableColumn*>(column_mut);
        DCHECK(nullable_column->data_column_raw_ptr()->is_struct());
        struct_column = down_cast<StructColumn*>(nullable_column->data_column_raw_ptr());
    } else {
        DCHECK(column->is_struct());
        DCHECK(!get_column_parquet_field()->is_nullable);
        struct_column = down_cast<StructColumn*>(column_mut);
    }
    ASSIGN_OR_RETURN(auto& field_col, struct_column->field_column(sub_field));
    auto ans = _child_readers[sub_field]->filter_dict_column(field_col, filter, sub_field_path, layer + 1);
    return ans;
}

Status StructColumnReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    auto* src_mut = src->as_mutable_raw_ptr();
    auto* dst_mut = dst->as_mutable_raw_ptr();
    StructColumn* struct_column_src = nullptr;
    StructColumn* struct_column_dst = nullptr;
    if (src->is_nullable()) {
        NullableColumn* nullable_column_src = down_cast<NullableColumn*>(src_mut);
        DCHECK(nullable_column_src->data_column_raw_ptr()->is_struct());
        struct_column_src = down_cast<StructColumn*>(nullable_column_src->data_column_raw_ptr());
        NullableColumn* nullable_column_dst = down_cast<NullableColumn*>(dst_mut);
        DCHECK(nullable_column_dst->data_column_raw_ptr()->is_struct());
        struct_column_dst = down_cast<StructColumn*>(nullable_column_dst->data_column_raw_ptr());
        nullable_column_dst->swap_null_column(*nullable_column_src);
    } else {
        DCHECK(src->is_struct());
        DCHECK(dst->is_struct());
        DCHECK(!get_column_parquet_field()->is_nullable);
        struct_column_src = down_cast<StructColumn*>(src_mut);
        struct_column_dst = down_cast<StructColumn*>(dst_mut);
    }
    const auto& field_names = struct_column_dst->field_names();
    for (size_t i = 0; i < field_names.size(); i++) {
        const auto& field_name = field_names[i];
        if (LIKELY(_child_readers.find(field_name) != _child_readers.end())) {
            if (_child_readers[field_name] == nullptr) {
                ASSIGN_OR_RETURN(auto* dst_field, struct_column_dst->field_column_raw_ptr(field_name));
                ASSIGN_OR_RETURN(auto* src_field, struct_column_src->field_column_raw_ptr(field_name));

                dst_field->swap_column(*src_field);
            } else {
                ASSIGN_OR_RETURN(auto& dst_field, struct_column_dst->field_column(field_name));
                ASSIGN_OR_RETURN(auto& src_field, struct_column_src->field_column(field_name));
                RETURN_IF_ERROR(_child_readers[field_name]->fill_dst_column(dst_field, src_field));
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
    if (subfield_expr->children().size() != 1 || !subfield_expr->get_child(0)->is_slotref()) {
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

// VariantColumnReader

Status VariantColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    auto* dst_mut = dst->as_mutable_raw_ptr();
    VariantColumn* variant_column = nullptr;
    NullableColumn* nullable_column = nullptr;
    if (dst->is_nullable()) {
        nullable_column = down_cast<NullableColumn*>(dst_mut);
        DCHECK(nullable_column->data_column_raw_ptr()->is_variant());
        variant_column = down_cast<VariantColumn*>(nullable_column->data_column_raw_ptr());
    } else {
        DCHECK(dst->is_variant());
        DCHECK(!get_column_parquet_field()->is_nullable);
        variant_column = down_cast<VariantColumn*>(dst_mut);
    }

    ColumnPtr metadata_col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    ColumnPtr value_col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    RETURN_IF_ERROR(_metadata_reader->read_range(range, filter, metadata_col));
    RETURN_IF_ERROR(_value_reader->read_range(range, filter, value_col));

    ColumnPtr typed_value_col;
    const NullableColumn* typed_value_nullable = nullptr;
    if (_has_typed_value) {
        typed_value_col = ColumnHelper::create_column(_typed_value_type, true);
        RETURN_IF_ERROR(_typed_value_reader->read_range(range, filter, typed_value_col));
        typed_value_nullable = down_cast<const NullableColumn*>(typed_value_col.get());
    }

    auto* metadata_nullable = down_cast<NullableColumn*>(metadata_col->as_mutable_raw_ptr());
    auto* value_nullable = down_cast<NullableColumn*>(value_col->as_mutable_raw_ptr());
    const auto* metadata_column = down_cast<const BinaryColumn*>(metadata_nullable->data_column().get());
    const auto* value_column = down_cast<const BinaryColumn*>(value_nullable->data_column().get());
    const auto& metadata_nulls = metadata_nullable->null_column()->get_data();
    const auto& value_nulls = value_nullable->null_column()->get_data();

    // Get definition levels to determine which variant groups are null
    level_t* def_levels = nullptr;
    level_t* rep_levels = nullptr;
    size_t num_levels = 0;
    _value_reader->get_levels(&def_levels, &rep_levels, &num_levels);
    // Use definition levels to determine null values
    const LevelInfo level_info = get_column_parquet_field()->level_info;

    // Verify metadata and value columns are aligned
    DCHECK_EQ(metadata_column->size(), value_column->size());
    DCHECK_EQ(metadata_nulls.size(), value_nulls.size());
    DCHECK_EQ(metadata_nulls.size(), metadata_column->size());

    // ScalarColumnReader returns a value for each row (including null values when parent group is null)
    // So metadata_column->size() should equal num_levels
    const size_t num_rows = metadata_column->size();
    if (typed_value_col != nullptr && typed_value_col->size() != num_rows) {
        return Status::InternalError("Typed value column size mismatch for variant reader");
    }
    variant_column->reserve(num_rows);

    if (def_levels != nullptr && num_levels > 0) {
        // For optional variant group, num_levels should equal num_rows
        DCHECK_EQ(num_levels, num_rows);

        for (size_t i = 0; i < num_levels; ++i) {
            const bool has_typed = typed_value_nullable != nullptr && !typed_value_nullable->is_null(i);
            // Check if metadata or value is null (which indicates variant group is null)
            if (!has_typed && (metadata_nulls[i] || value_nulls[i])) {
                // Usually when metadata/value are null, def_level should be less than max_def_level
                // But there may be edge cases in parquet encoding, so just log a warning instead of DCHECK
                if (def_levels[i] >= level_info.max_def_level) {
                    VLOG_FILE << "Null metadata/value at row " << i
                              << " but variant group marked as non-null (def_level=" << def_levels[i]
                              << " >= max_def_level=" << level_info.max_def_level << ")";
                }
                variant_column->append(VariantRowValue::from_null());
            } else if (def_levels[i] >= level_info.max_def_level) {
                // Variant group exists, prefer typed_value when available
                const Slice metadata_slice = metadata_column->get_slice(i);
                const Slice value_slice = value_column->get_slice(i);

                // Apache Parquet Variant Shredding Spec:
                // value=null, typed_value=null    → Missing value (valid only for object fields)
                // value=non-null, typed_value=null → Value present, use raw variant encoding
                // value=null, typed_value=non-null → Value present, encode from shredded type
                // value=non-null, typed_value=non-null → Partially shredded object (both fields used)
                //
                // For primitives/arrays: value and typed_value must be mutually exclusive
                // For objects: both can be non-null (typed_value has shredded fields, value has non-shredded fields)
                if (has_typed) {
                    // Validation: Check for spec violations (both non-null for non-objects)
                    if (!value_slice.empty() && !_typed_value_type.is_struct_type()) {
                        VLOG_FILE << "Warning: Both value and typed_value are non-null at row " << i
                                  << " for non-object type " << type_to_string(_typed_value_type.type)
                                  << ". Per Parquet Variant Shredding spec, this should only occur for objects. "
                                  << "Using typed_value.";
                    }

                    VariantMetadata meta(metadata_slice);
                    RETURN_IF_ERROR(_ctx.use_metadata(meta));

                    // For partially shredded objects (both value and typed_value non-null),
                    // we need to pass both columns to the encoder for merging
                    if (!value_slice.empty() && _typed_value_type.is_struct_type()) {
                        // Construct wrapper struct with value and typed_value fields
                        Columns fields{value_col, typed_value_col};
                        auto wrapper_col = StructColumn::create(fields, {"value", "typed_value"});
                        TypeDescriptor wrapper_type;
                        wrapper_type.type = TYPE_STRUCT;
                        wrapper_type.field_names = {"value", "typed_value"};
                        wrapper_type.children.emplace_back(TYPE_VARBINARY);
                        wrapper_type.children.emplace_back(_typed_value_type);

                        auto variant = VariantEncoder::encode_shredded_column_row(wrapper_col, wrapper_type, i, &_ctx);
                        if (!variant.ok()) {
                            variant_column->append(VariantRowValue::from_null());
                            continue;
                        }
                        variant_column->append(variant.value());
                    } else {
                        // Only typed_value is present, encode directly
                        auto variant = VariantEncoder::encode_shredded_column_row(typed_value_col, _typed_value_type, i,
                                                                                  &_ctx);
                        if (!variant.ok()) {
                            variant_column->append(VariantRowValue::from_null());
                            continue;
                        }
                        variant_column->append(variant.value());
                    }
                    continue;
                }

                // Even if null flags are false, slices can be empty (empty strings are valid non-null values in BinaryColumn)
                // But Variant requires non-empty value, so treat empty slices as null
                if (metadata_slice.empty() || value_slice.empty()) {
                    // value slice probably is empty since it's been filtered out during encoding according to `filter`
                    // VLOG_FILE << "Empty metadata or value slice at row " << i
                    //           << " (metadata_size=" << metadata_slice.size << ", value_size=" << value_slice.size
                    //           << "), treating as null variant";
                    variant_column->append(VariantRowValue::from_null());
                } else if (auto variant = VariantRowValue::create(metadata_slice, value_slice); !variant.ok()) {
                    // Read malformed variant value as null
                    VLOG_FILE << "Failed to create variant value at row " << i << ": " << variant.status();
                    variant_column->append(VariantRowValue::from_null());
                } else {
                    variant_column->append(variant.value());
                }
            } else {
                // Variant group is null, metadata and value should also be null
                if (!metadata_nulls[i] || !value_nulls[i]) {
                    VLOG_FILE << "Null variant group at row " << i
                              << " but metadata/value not marked as null (metadata_null=" << metadata_nulls[i]
                              << ", value_null=" << value_nulls[i] << ")";
                }
                variant_column->append(VariantRowValue::from_null());
            }
        }

        // Verify we produced the expected number of rows
        DCHECK_EQ(variant_column->size(), num_levels)
                << "Variant column size mismatch: expected " << num_levels << ", got " << variant_column->size();
    } else {
        // Variant group is required, so all rows should have valid data (but fields can still be null)
        for (size_t i = 0; i < num_rows; ++i) {
            const bool has_typed = typed_value_nullable != nullptr && !typed_value_nullable->is_null(i);
            const Slice metadata_slice = metadata_column->get_slice(i);
            const Slice value_slice = value_column->get_slice(i);
            if (!has_typed && (metadata_nulls[i] || value_nulls[i])) {
                // Even for required variant group, metadata/value fields can be null
                variant_column->append(VariantRowValue::from_null());
            } else if (has_typed) {
                // Validation: Check for spec violations (both non-null for non-objects)
                if (!value_slice.empty() && !_typed_value_type.is_struct_type()) {
                    VLOG_FILE << "Warning: Both value and typed_value are non-null at row " << i
                              << " for non-object type " << type_to_string(_typed_value_type.type)
                              << ". Per Parquet Variant Shredding spec, this should only occur for objects. "
                              << "Using typed_value.";
                }

                VariantMetadata meta(metadata_slice);
                RETURN_IF_ERROR(_ctx.use_metadata(meta));

                // For partially shredded objects (both value and typed_value non-null),
                // we need to pass both columns to the encoder for merging
                if (!value_slice.empty() && _typed_value_type.is_struct_type()) {
                    // Construct wrapper struct with value and typed_value fields
                    Columns fields{value_col, typed_value_col};
                    auto wrapper_col = StructColumn::create(fields, {"value", "typed_value"});
                    TypeDescriptor wrapper_type;
                    wrapper_type.type = TYPE_STRUCT;
                    wrapper_type.field_names = {"value", "typed_value"};
                    wrapper_type.children.emplace_back(TYPE_VARBINARY);
                    wrapper_type.children.emplace_back(_typed_value_type);

                    auto variant = VariantEncoder::encode_shredded_column_row(wrapper_col, wrapper_type, i, &_ctx);
                    if (!variant.ok()) {
                        variant_column->append(VariantRowValue::from_null());
                        continue;
                    }
                    variant_column->append(variant.value());
                } else {
                    // Only typed_value is present, encode directly
                    auto variant =
                            VariantEncoder::encode_shredded_column_row(typed_value_col, _typed_value_type, i, &_ctx);
                    if (!variant.ok()) {
                        variant_column->append(VariantRowValue::from_null());
                        continue;
                    }
                    variant_column->append(variant.value());
                }
            } else {
                // Even if null flags are false, slices can be empty (empty strings are valid non-null values in BinaryColumn)
                // But Variant requires non-empty value, so treat empty slices as null
                if (metadata_slice.empty() || value_slice.empty()) {
                    VLOG_FILE << "Empty metadata or value slice at row " << i
                              << " (metadata_size=" << metadata_slice.size << ", value_size=" << value_slice.size
                              << "), treating as null variant";
                    variant_column->append(VariantRowValue::from_null());
                } else if (auto variant = VariantRowValue::create(metadata_slice, value_slice); !variant.ok()) {
                    VLOG_FILE << "Failed to create variant value at row " << i << ": " << variant.status();
                    variant_column->append(VariantRowValue::from_null());
                } else {
                    variant_column->append(variant.value());
                }
            }
        }

        // Verify we produced the expected number of rows
        DCHECK_EQ(variant_column->size(), num_rows)
                << "Variant column size mismatch: expected " << num_rows << ", got " << variant_column->size();
    }

    // Handle nullable column null flags
    if (dst->is_nullable()) {
        DCHECK(nullable_column != nullptr);
        DCHECK_EQ(variant_column->size(), num_rows)
                << "Variant column size must equal num_rows before setting nullable flags";

        if (def_levels != nullptr && num_levels > 0) {
            NullColumn null_column(num_levels);
            auto& is_nulls = null_column.get_data();
            bool has_null = false;

            for (size_t i = 0; i < num_levels; ++i) {
                if (def_levels[i] >= level_info.max_def_level) {
                    is_nulls[i] = 0; // Variant group exists (at parquet level)
                    // Note: Even if variant group exists, we may still have null metadata/value or empty slices,
                    // which result in null variants at the application level
                    if (metadata_nulls[i] || value_nulls[i]) {
                        VLOG_ROW << "Variant group marked as non-null at row " << i
                                 << " but has null metadata/value fields";
                    }
                } else {
                    is_nulls[i] = 1; // Variant group is null
                    has_null = true;
                    // Verify consistency: null group should usually have null metadata/value
                    if (!metadata_nulls[i] || !value_nulls[i]) {
                        VLOG_ROW << "Null variant group at row " << i
                                 << " but metadata/value not marked as null (metadata_null=" << metadata_nulls[i]
                                 << ", value_null=" << value_nulls[i] << ")";
                    }
                }
            }

            nullable_column->null_column_raw_ptr()->swap_column(null_column);
            nullable_column->set_has_null(has_null);

            // Final verification
            DCHECK_EQ(nullable_column->size(), num_levels) << "Final nullable column size mismatch";
        } else {
            NullColumn null_column(num_rows, 0);
            nullable_column->null_column_raw_ptr()->swap_column(null_column);
            nullable_column->set_has_null(false);

            // Final verification
            DCHECK_EQ(nullable_column->size(), num_rows)
                    << "Final nullable column size mismatch for required variant group";
        }
    } else {
        // Non-nullable variant column
        DCHECK_EQ(variant_column->size(), num_rows) << "Final variant column size must equal num_rows";
    }

    return Status::OK();
}

} // namespace starrocks::parquet
