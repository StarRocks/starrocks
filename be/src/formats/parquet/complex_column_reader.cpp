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

#include <algorithm>
#include <optional>

#include "base/string/slice.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/variant_builder.h"
#include "column/variant_column.h"
#include "column/variant_encoder.h"
#include "exprs/literal.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/schema.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "storage/column_expr_predicate.h"
#include "types/variant_value.h"

namespace starrocks::parquet {

// File-scope helper — avoids repeated construction and deduplicate the several
// `static const TypeDescriptor k_variant_type` locals scattered through the file.
static const TypeDescriptor& variant_type_desc() {
    static const TypeDescriptor k = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT);
    return k;
}

static const TypeDescriptor& array_varbinary_type_desc() {
    static const TypeDescriptor k = TypeDescriptor::create_array_type(TYPE_VARBINARY_DESC);
    return k;
}

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
            return Status::InternalError(strings::Substitute("there is no match subfield reader for $0", field_name));
        }
    }

    if (UNLIKELY(first_read)) {
        return Status::InternalError(strings::Substitute("All used subfield of struct type $0 is not exist",
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
            return Status::InternalError(strings::Substitute("there is no match subfield reader for $0", field_name));
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
        if (!ret.ok()) {
            LOG(WARNING) << "row_group_zone_map_filter failed, skipping filter: " << ret.status().to_string();
            return false;
        }

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
        if (!ret.ok()) {
            LOG(WARNING) << "page_index_zone_map_filter failed, skipping filter: " << ret.status().to_string();
            return false;
        }

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

// ==================================================================
// VariantColumnReader
// ==================================================================

static Status _prepare_shredded_field_node(ShreddedFieldNode* node) {
    if (node == nullptr) {
        return Status::InvalidArgument("node should not be null");
    }
    if (node->value_reader != nullptr) {
        RETURN_IF_ERROR(node->value_reader->prepare());
    }
    if (node->typed_value_reader != nullptr) {
        RETURN_IF_ERROR(node->typed_value_reader->prepare());
    }
    if (node->array_element_value_reader != nullptr) {
        RETURN_IF_ERROR(node->array_element_value_reader->prepare());
    }
    for (auto& child : node->children) {
        RETURN_IF_ERROR(_prepare_shredded_field_node(&child));
    }
    return Status::OK();
}

static Status _read_shredded_field_node(const Range<uint64_t>& range, const Filter* filter, ShreddedFieldNode* node) {
    if (node == nullptr) {
        return Status::InvalidArgument("node should not be null");
    }
    if (node->value_reader != nullptr) {
        node->value_column = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        RETURN_IF_ERROR(node->value_reader->read_range(range, filter, node->value_column));
    } else {
        node->value_column = nullptr;
    }
    if (node->typed_value_reader != nullptr) {
        node->typed_value_column = ColumnHelper::create_column(*node->typed_value_read_type, true);
        RETURN_IF_ERROR(node->typed_value_reader->read_range(range, filter, node->typed_value_column));
    } else {
        node->typed_value_column = nullptr;
    }
    if (node->array_element_value_reader != nullptr) {
        node->array_element_value_column = ColumnHelper::create_column(array_varbinary_type_desc(), true);
        RETURN_IF_ERROR(node->array_element_value_reader->read_range(range, filter, node->array_element_value_column));
    } else {
        node->array_element_value_column = nullptr;
    }
    // Sanity check: value and typed_value columns must have the same number of rows.
    // A mismatch indicates a corrupted file or a filter/range application bug.
    if (node->value_column != nullptr && node->typed_value_column != nullptr &&
        node->value_column->size() != node->typed_value_column->size()) {
        return Status::InternalError(
                strings::Substitute("shredded field '$0': value_column size $1 != typed_value_column size $2",
                                    node->name, node->value_column->size(), node->typed_value_column->size()));
    }
    if (node->typed_value_column != nullptr && node->array_element_value_column != nullptr &&
        node->typed_value_column->size() != node->array_element_value_column->size()) {
        return Status::InternalError(strings::Substitute(
                "shredded field '$0': typed_value_column size $1 != array_element_value_column size $2", node->name,
                node->typed_value_column->size(), node->array_element_value_column->size()));
    }
    for (auto& child : node->children) {
        RETURN_IF_ERROR(_read_shredded_field_node(range, filter, &child));
    }
    return Status::OK();
}

static void _collect_shredded_field_io_range(const ShreddedFieldNode& node,
                                             std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                             int64_t* end_offset, ColumnIOTypeFlags types, bool active) {
    if (node.value_reader != nullptr) {
        node.value_reader->collect_column_io_range(ranges, end_offset, types, active);
    }
    if (node.typed_value_reader != nullptr) {
        node.typed_value_reader->collect_column_io_range(ranges, end_offset, types, active);
    }
    if (node.array_element_value_reader != nullptr) {
        node.array_element_value_reader->collect_column_io_range(ranges, end_offset, types, active);
    }
    for (const auto& child : node.children) {
        _collect_shredded_field_io_range(child, ranges, end_offset, types, active);
    }
}

static void _select_shredded_field_offset_index(const ShreddedFieldNode& node, const SparseRange<uint64_t>& range,
                                                const uint64_t rg_first_row) {
    if (node.value_reader != nullptr) {
        node.value_reader->select_offset_index(range, rg_first_row);
    }
    if (node.typed_value_reader != nullptr) {
        node.typed_value_reader->select_offset_index(range, rg_first_row);
    }
    if (node.array_element_value_reader != nullptr) {
        node.array_element_value_reader->select_offset_index(range, rg_first_row);
    }
    for (const auto& child : node.children) {
        _select_shredded_field_offset_index(child, range, rg_first_row);
    }
}

// Maximum nesting depth for mutually-recursive array overlay reconstruction.
// Prevents stack-overflow on pathological / malformed Parquet files where
// typed_value fields nest arrays inside arrays beyond reasonable depth.
static constexpr int kMaxShreddedArrayNestingDepth = 32;

// Collect overlays for one ARRAY element row from shredded child nodes recursively.
// Priority per path: typed scalar value > fallback binary value.
// Returns true for traversal completion; parse/append failures are ignored per-field.
static bool _collect_overlays_for_array_element(size_t element_row, const std::vector<ShreddedFieldNode>& nodes,
                                                std::string_view metadata_raw,
                                                std::vector<VariantBuilder::Overlay>& overlays, int depth = 0);

// Rebuild one ARRAY value for `row` by merging:
// 1) typed array elements (and their child overlays), and
// 2) fallback base array binary from remain column when present.
// Missing elements are filled with null to keep array positions stable.
//
// Layout distinction:
// - ARRAY with shredded children: Each array element may contain shredded sub-paths (e.g., array of objects
//   with shredded fields). We collect overlays per element and use VariantBuilder to reconstruct.
// - ARRAY without children (fully-typed scalar array): The typed_value_column holds the array data directly,
//   with no shredded sub-paths. We extract elements directly from the typed array's elements_column.
static StatusOr<VariantRowValue> _rebuild_array_overlay(size_t row, const ShreddedFieldNode& array_node,
                                                        std::string_view metadata_raw, std::string_view base_array_raw,
                                                        int depth = 0) {
    if (depth > kMaxShreddedArrayNestingDepth) {
        LOG(WARNING) << "variant shredded array nesting depth exceeded limit (" << kMaxShreddedArrayNestingDepth
                     << ") at path='" << array_node.full_path << "'; falling back to base binary";
        return Status::ResourceBusy("shredded array nesting depth limit exceeded");
    }
    const Column* typed_col = nullptr;
    size_t typed_row = 0;
    if (!ParquetUtils::get_non_null_data_column_and_row(array_node.typed_value_column.get(), row, &typed_col,
                                                        &typed_row) ||
        !typed_col->is_array()) {
        return Status::NotFound("array typed_value is null");
    }
    const auto* typed_array = down_cast<const ArrayColumn*>(typed_col);
    const auto& typed_offsets = typed_array->offsets().get_data();
    const uint32_t typed_begin = typed_offsets[typed_row];
    const uint32_t typed_end = typed_offsets[typed_row + 1];
    const uint32_t typed_count = typed_end - typed_begin;

    VariantRowRef base_row(metadata_raw, base_array_raw);
    VariantValue base_value = base_row.get_value();
    uint32_t base_count = 0;
    if (base_value.type() == VariantType::ARRAY) {
        auto n = base_value.num_elements();
        if (n.ok()) {
            base_count = n.value();
        }
    }
    const uint32_t total_elements = std::max<uint32_t>(typed_count, base_count);

    VariantArrayBuilder array_builder;

    // Two distinct reconstruction paths based on whether shredded children exist.
    if (!array_node.children.empty()) {
        // Path 1: Array with shredded children (e.g., array of objects with shredded sub-paths).
        // Each element may have overlays from child nodes. We collect overlays per element
        // and use VariantBuilder to merge with fallback base elements.
        //
        // There are two possible sources for the per-element base object here:
        // 1. outer array `value` binary (`base_array_raw`) if the whole array kept a fallback payload
        // 2. rewritten typed_value reader as ARRAY<VARBINARY>, where each typed element is actually
        //    `list.element.value` from the object-array layout
        //
        // The second source is the important special case for array<object> shredding. Once
        // ColumnReaderFactory rewrites the node reader to `element.value`, `typed_count` still
        // describes the array cardinality, but the typed elements are no longer "typed objects";
        // they are the per-element base variant payloads that child overlays should attach to.
        const bool typed_elements_are_variant_binary =
                array_node.typed_value_read_type != nullptr && array_node.typed_value_read_type->type == TYPE_ARRAY &&
                array_node.typed_value_read_type->children.size() == 1 &&
                array_node.typed_value_read_type->children[0].type == TYPE_VARBINARY;
        const Column* typed_elements = typed_array->elements_column().get();
        for (uint32_t i = 0; i < total_elements; ++i) {
            std::optional<VariantRowRef> base_element;
            // Object-array rewrite changes typed_value_reader to ARRAY<VARBINARY> over element.value.
            // In that layout, each typed array element is the base object payload and must be merged
            // with shredded child overlays before consulting the outer array fallback.
            if (typed_elements_are_variant_binary && i < typed_count) {
                Slice typed_element_slice;
                if (ColumnHelper::get_binary_slice_at(typed_elements, typed_begin + i, &typed_element_slice)) {
                    base_element.emplace(std::string_view(metadata_raw),
                                         std::string_view(typed_element_slice.data, typed_element_slice.size));
                }
            }
            if (i < base_count) {
                auto base_element_status = base_value.get_element_at_index(base_row.get_metadata(), i);
                if (!base_element.has_value() && base_element_status.ok()) {
                    base_element.emplace(VariantRowRef::from_variant(base_row.get_metadata(),
                                                                     std::move(base_element_status).value()));
                }
            }

            std::vector<VariantBuilder::Overlay> element_overlays;
            if (i < typed_count) {
                _collect_overlays_for_array_element(typed_begin + i, array_node.children, metadata_raw,
                                                    element_overlays, depth + 1);
            }

            if (!element_overlays.empty() || base_element.has_value()) {
                auto built = VariantBuilder::build_row_from_overlays(base_element, std::move(element_overlays));
                if (built.ok()) {
                    array_builder.add(std::move(built).value());
                    continue;
                }
                // Build failed: fall back to the raw base element if available rather than
                // losing data entirely. This can happen when overlay encoding fails but the
                // pre-shredded binary is still valid.
                if (base_element.has_value()) {
                    array_builder.add(base_element->to_owned());
                    continue;
                }
            }
            array_builder.add_null();
        }
    } else {
        // Path 2: Fully-typed scalar array without shredded children.
        // Since there are no shredded sub-paths, there are no overlays to collect.
        // We extract array elements directly from the typed array column.
        //
        // Example: A shredded INTEGER array like [1, 2, 3] has:
        //   - typed_value_column holding the array data
        //   - empty children (scalar elements have no sub-paths)
        // In this case, we directly encode each typed array element.
        if (array_node.typed_value_read_type == nullptr || array_node.typed_value_read_type->type != TYPE_ARRAY ||
            array_node.typed_value_read_type->children.empty()) {
            return Status::InternalError("variant shredded array node has no element type descriptor");
        }
        const auto& element_type = array_node.typed_value_read_type->children[0];
        const auto* typed_elements = typed_array->elements_column().get();

        // Scalar-array layout (list.element.{value,typed_value(scalar)}) may carry element-level
        // fallback in element.value even when top-level base array binary is absent.
        // Unlike Path 1 above, there are no child overlays here; element.value is only used when
        // encoding the scalar typed element fails or the typed element is null for a given position.
        uint32_t array_element_begin = 0;
        uint32_t array_element_count = 0;
        const Column* array_element_values = nullptr;
        if (array_node.scalar_array_layout && array_node.array_element_value_column != nullptr) {
            const Column* element_value_col = nullptr;
            size_t element_value_row = 0;
            if (ParquetUtils::get_non_null_data_column_and_row(array_node.array_element_value_column.get(), row,
                                                               &element_value_col, &element_value_row) &&
                element_value_col->is_array()) {
                const auto* element_value_array = down_cast<const ArrayColumn*>(element_value_col);
                const auto& element_value_offsets = element_value_array->offsets().get_data();
                array_element_begin = element_value_offsets[element_value_row];
                array_element_count = element_value_offsets[element_value_row + 1] - array_element_begin;
                array_element_values = element_value_array->elements_column().get();
            }
        }

        const uint32_t total = std::max<uint32_t>(std::max<uint32_t>(typed_count, array_element_count), base_count);
        for (uint32_t i = 0; i < total; ++i) {
            // First, try to use the typed value directly (common case for fully-typed arrays).
            if (i < typed_count) {
                auto element_value = VariantEncoder::encode_datum(typed_elements->get(typed_begin + i), element_type);
                if (element_value.ok()) {
                    array_builder.add(std::move(element_value).value());
                    continue;
                }
            }

            // For scalar-array layout, fallback to element.value first.
            if (i < array_element_count && array_element_values != nullptr) {
                Slice array_element_slice;
                if (ColumnHelper::get_binary_slice_at(array_element_values, array_element_begin + i,
                                                      &array_element_slice)) {
                    array_builder.add(
                            VariantRowValue(std::string_view(metadata_raw),
                                            std::string_view(array_element_slice.data, array_element_slice.size)));
                    continue;
                }
            }

            // Fall back to base element if typed value is missing/invalid.
            if (i < base_count) {
                auto base_element_status = base_value.get_element_at_index(base_row.get_metadata(), i);
                if (base_element_status.ok()) {
                    array_builder.add(VariantRowValue::from_variant(base_row.get_metadata(),
                                                                    std::move(base_element_status).value()));
                    continue;
                }
            }

            array_builder.add_null();
        }
    }
    return array_builder.build();
}

// Recursive worker used by ARRAY reconstruction to collect per-element overlays.
// It walks child shredded nodes, preferring typed scalar data and falling back to remain-binary slices.
static bool _collect_overlays_for_array_element(size_t element_row, const std::vector<ShreddedFieldNode>& nodes,
                                                std::string_view metadata_raw,
                                                std::vector<VariantBuilder::Overlay>& overlays, int depth) {
    if (depth > kMaxShreddedArrayNestingDepth) {
        LOG(WARNING) << "variant shredded array element nesting depth exceeded limit (" << kMaxShreddedArrayNestingDepth
                     << "); skipping sub-overlays";
        return false;
    }
    for (const auto& node : nodes) {
        if (node.typed_kind == ShreddedTypedKind::SCALAR && node.typed_value_column != nullptr) {
            const Column* typed_col = nullptr;
            size_t typed_row = 0;
            if (ParquetUtils::get_non_null_data_column_and_row(node.typed_value_column.get(), element_row, &typed_col,
                                                               &typed_row)) {
                auto typed_value = VariantEncoder::encode_datum(typed_col->get(typed_row), *node.typed_value_read_type);
                if (typed_value.ok()) {
                    overlays.emplace_back(VariantBuilder::Overlay{.path = node.parsed_full_path,
                                                                  .value = std::move(typed_value).value()});
                    continue;
                }
            }
            // Typed encode failed or value is null: fall through to binary fallback below.
        }

        if (node.typed_kind == ShreddedTypedKind::ARRAY && node.typed_value_column != nullptr) {
            // Nested array sub-field within an array element (e.g., array-of-objects where
            // one field is itself a shredded array). Reconstruct recursively.
            Slice fallback_slice;
            std::string_view base_array_raw = VariantValue::kEmptyValue;
            if (ColumnHelper::get_binary_slice_at(node.value_column.get(), element_row, &fallback_slice)) {
                base_array_raw = std::string_view(fallback_slice.data, fallback_slice.size);
            }
            auto array_overlay = _rebuild_array_overlay(element_row, node, metadata_raw, base_array_raw, depth + 1);
            if (array_overlay.ok()) {
                overlays.emplace_back(VariantBuilder::Overlay{.path = node.parsed_full_path,
                                                              .value = std::move(array_overlay).value()});
            } else if (base_array_raw != VariantValue::kEmptyValue) {
                overlays.emplace_back(VariantBuilder::Overlay{.path = node.parsed_full_path,
                                                              .value = VariantRowValue(metadata_raw, base_array_raw)});
            }
            continue;
        }

        // NONE node (or SCALAR/ARRAY with no typed data): emit fallback binary then recurse children.
        Slice fallback_slice;
        if (ColumnHelper::get_binary_slice_at(node.value_column.get(), element_row, &fallback_slice)) {
            overlays.emplace_back(VariantBuilder::Overlay{
                    .path = node.parsed_full_path,
                    .value = VariantRowValue(std::string_view(metadata_raw),
                                             std::string_view(fallback_slice.data, fallback_slice.size))});
        }
        if (!node.children.empty()) {
            _collect_overlays_for_array_element(element_row, node.children, metadata_raw, overlays, depth + 1);
        }
    }
    return true;
}

static bool _collect_overlays_for_row(size_t row, std::string_view metadata_raw, const ShreddedFieldNode& node,
                                      std::vector<VariantBuilder::Overlay>& overlays) {
    if (node.typed_kind == ShreddedTypedKind::SCALAR && node.typed_value_column != nullptr) {
        const Column* typed_col = nullptr;
        size_t typed_row = 0;
        if (ParquetUtils::get_non_null_data_column_and_row(node.typed_value_column.get(), row, &typed_col,
                                                           &typed_row)) {
            auto typed_value = VariantEncoder::encode_datum(typed_col->get(typed_row), *node.typed_value_read_type);
            if (typed_value.ok()) {
                overlays.emplace_back(VariantBuilder::Overlay{.path = node.parsed_full_path,
                                                              .value = std::move(typed_value).value()});
                return true;
            }
            // encode_datum failed (e.g., type mismatch or overflow). Fall through to binary fallback.
            VLOG(3) << "variant shredded scalar encode failed for path '" << node.full_path
                    << "'; falling back to binary value";
        }
        Slice fallback_slice;
        if (ColumnHelper::get_binary_slice_at(node.value_column.get(), row, &fallback_slice)) {
            overlays.emplace_back(VariantBuilder::Overlay{
                    .path = node.parsed_full_path,
                    .value = VariantRowValue(std::string_view(metadata_raw),
                                             std::string_view(fallback_slice.data, fallback_slice.size))});
            return true;
        }
        // No typed value and no fallback binary: field is absent for this row (valid per spec).
        return true;
    }

    if (node.typed_kind == ShreddedTypedKind::ARRAY && node.typed_value_column != nullptr) {
        Slice fallback_slice;
        std::string_view base_array_raw = VariantValue::kEmptyValue;
        if (ColumnHelper::get_binary_slice_at(node.value_column.get(), row, &fallback_slice)) {
            base_array_raw = std::string_view(fallback_slice.data, fallback_slice.size);
        }
        auto array_overlay = _rebuild_array_overlay(row, node, metadata_raw, base_array_raw);
        if (array_overlay.ok()) {
            overlays.emplace_back(
                    VariantBuilder::Overlay{.path = node.parsed_full_path, .value = std::move(array_overlay).value()});
            return true;
        }
        if (base_array_raw != VariantValue::kEmptyValue) {
            overlays.emplace_back(VariantBuilder::Overlay{.path = node.parsed_full_path,
                                                          .value = VariantRowValue(metadata_raw, base_array_raw)});
        }
        return true;
    }

    // NONE node (partially-shredded struct): combine fallback binary with shredded children.
    //
    // Per spec, the `value` binary for a partially-shredded object contains ONLY the
    // non-shredded fields; shredded fields are exclusively in typed_value children.
    // Keys are therefore disjoint — no field can appear in both.
    //
    // We emit overlays in order:
    //   1. Parent path "a" → fallback binary {non-shredded fields}
    //   2. Child paths "a.b", "a.c" → typed shredded values
    //
    // VariantBuilder::apply_overlay navigates into the EXISTING "a" object node when
    // processing "a.b", so child overlays are MERGED INTO the decoded parent binary,
    // not conflicting with it. This ordering (parent before children) is mandatory.
    Slice fallback_slice;
    if (ColumnHelper::get_binary_slice_at(node.value_column.get(), row, &fallback_slice)) {
        overlays.emplace_back(VariantBuilder::Overlay{
                .path = node.parsed_full_path,
                .value = VariantRowValue(std::string_view(metadata_raw),
                                         std::string_view(fallback_slice.data, fallback_slice.size))});
    }
    for (const auto& child : node.children) {
        _collect_overlays_for_row(row, metadata_raw, child, overlays);
    }
    return true;
}

struct TopBinding {
    enum class Kind : uint8_t { SCALAR = 0, VARIANT = 1 };
    Kind kind = Kind::SCALAR;
    std::string path;
    TypeDescriptor type;
    const ShreddedFieldNode* node = nullptr;
};

static const ShreddedFieldNode* find_node_by_path(const std::vector<ShreddedFieldNode>& nodes,
                                                  const std::string& target_path) {
    for (const auto& node : nodes) {
        if (node.full_path == target_path) return &node;
        if (!node.children.empty()) {
            auto* found = find_node_by_path(node.children, target_path);
            if (found != nullptr) return found;
        }
    }
    return nullptr;
}

// Auto-discover binding paths from the shredded_fields tree when no explicit shredded_paths are
// provided.  Stops at ARRAY boundaries (does not recurse into array element children) and at SCALAR
// leaves.  Struct-like NONE nodes are recursed.
static void collect_all_top_binding_paths(const std::vector<ShreddedFieldNode>& nodes,
                                          std::vector<std::string>* paths) {
    for (const auto& node : nodes) {
        if (node.typed_kind != ShreddedTypedKind::NONE) {
            // SCALAR leaf or ARRAY boundary — emit and stop recursing.
            paths->push_back(node.full_path);
        } else if (!node.children.empty()) {
            // Struct-like grouping node — recurse into children.
            collect_all_top_binding_paths(node.children, paths);
        }
        // NONE node with no children: pure remain-binary; no typed binding needed.
    }
}

// Guided by shredded_paths: each path maps to SCALAR or VARIANT typed_column entry.
// If a requested path is not found in current file/row-group shredded fields, keep it as VARIANT
// with null node so output typed_columns keep request-shape stability.
// When shredded_paths is empty, all paths are auto-discovered from the nodes tree.
static void collect_top_bindings(const std::vector<ShreddedFieldNode>& nodes,
                                 const std::vector<std::string>& shredded_paths, std::vector<TopBinding>* out) {
    if (out == nullptr) {
        return;
    }
    std::vector<std::string> auto_paths;
    const std::vector<std::string>* effective_paths = &shredded_paths;
    if (shredded_paths.empty()) {
        collect_all_top_binding_paths(nodes, &auto_paths);
        effective_paths = &auto_paths;
        if (effective_paths->empty()) return;
    }
    for (const auto& path : *effective_paths) {
        const ShreddedFieldNode* node = find_node_by_path(nodes, path);
        if (node == nullptr) {
            // Path requested but not shredded in this file/RG: keep requested typed path.
            out->push_back(
                    {.kind = TopBinding::Kind::VARIANT, .path = path, .type = variant_type_desc(), .node = nullptr});
            continue;
        }
        if (node->typed_kind == ShreddedTypedKind::SCALAR && node->typed_value_column != nullptr) {
            out->push_back({.kind = TopBinding::Kind::SCALAR,
                            .path = path,
                            .type = *node->typed_value_read_type,
                            .node = node});
        } else {
            // Array boundary / struct children / fallback-only: pack as plain VariantColumn.
            out->push_back(
                    {.kind = TopBinding::Kind::VARIANT, .path = path, .type = variant_type_desc(), .node = node});
        }
    }
}

// NOTE – type demotion design tradeoff:
// When a SCALAR binding has both typed and fallback values in the same batch (mixed case),
// the output column type is demoted from the scalar type to VARIANT. This means the output
// column type for a given path can vary across row groups within the same scan. Downstream
// aggregation or projection that relies on a stable column type may see type mismatches.
// The correct long-term fix is to "promise" a stable output type at scan planning time and
// handle the fallback encoding within the typed column append, rather than changing the
// column type per batch. Left as a known limitation for now.
static std::vector<TopBinding> select_materialized_bindings(const std::vector<TopBinding>& input, size_t num_rows) {
    std::vector<TopBinding> output;
    output.reserve(input.size());
    for (const auto& binding : input) {
        if (binding.kind == TopBinding::Kind::SCALAR) {
            if (binding.node == nullptr) {
                continue;
            }
            VariantScalarMaterializeMode mode = decide_variant_scalar_materialize_mode(binding.node, num_rows);
            if (mode == VariantScalarMaterializeMode::KEEP_SCALAR) {
                // Fully-typed path: keep scalar materialization.
                output.push_back(binding);
            } else if (mode == VariantScalarMaterializeMode::DEMOTE_VARIANT) {
                // Mixed or fallback-only path: demote to VARIANT (see NOTE above).
                TopBinding variant_binding = binding;
                variant_binding.kind = TopBinding::Kind::VARIANT;
                variant_binding.type = variant_type_desc();
                output.push_back(std::move(variant_binding));
            } else {
                LOG_EVERY_N(WARNING, 100) << "drop scalar shredded binding due to missing node, path=" << binding.path;
            }
        } else {
            // VARIANT: always include; reconstruction is always valid.
            output.push_back(binding);
        }
    }
    return output;
}

VariantScalarMaterializeMode decide_variant_scalar_materialize_mode(const ShreddedFieldNode* node, size_t num_rows) {
    if (node == nullptr) {
        return VariantScalarMaterializeMode::DROP;
    }
    const bool has_typed = ParquetUtils::has_non_null_value(node->typed_value_column.get(), num_rows);
    const bool has_fallback = ParquetUtils::has_non_null_binary_value(node->value_column.get(), num_rows);
    if (has_typed && !has_fallback) {
        return VariantScalarMaterializeMode::KEEP_SCALAR;
    }
    if (has_typed || has_fallback) {
        return VariantScalarMaterializeMode::DEMOTE_VARIANT;
    }
    // Keep all-null scalar bindings to preserve a stable shredded-path shape.
    // append_top_scalar_binding_value() will append null for every row in this batch.
    return VariantScalarMaterializeMode::KEEP_SCALAR;
}

static void append_top_scalar_binding_value(size_t row, const TopBinding& binding, Column* dst_column) {
    if (dst_column == nullptr || binding.node == nullptr) {
        return;
    }
    const Column* typed_col = nullptr;
    size_t typed_row = 0;
    if (ParquetUtils::get_non_null_data_column_and_row(binding.node->typed_value_column.get(), row, &typed_col,
                                                       &typed_row)) {
        dst_column->append_datum(typed_col->get(typed_row));
    } else {
        dst_column->append_nulls(1);
    }
}

// Append one row of a VARIANT binding to dst (a NullableColumn<VariantColumn>).
// For ARRAY nodes: reconstructs the full array binary via _rebuild_array_overlay (file-local).
// For other nodes (struct/fallback-only): uses the node's value_column fallback binary.
// dst is kept in object mode (VariantRowValue per entry); no nested typed_columns inside.
static void append_variant_binding_row_from_built_row(size_t row, const TopBinding& binding,
                                                      std::string_view raw_metadata, std::string_view built_metadata,
                                                      std::string_view built_value, Column* dst) {
    if (dst == nullptr) return;
    auto* nullable = down_cast<NullableColumn*>(dst);
    auto* inner_variant = down_cast<VariantColumn*>(nullable->data_column()->as_mutable_raw_ptr());

    auto append_value = [&](const VariantRowValue& rv) {
        inner_variant->append(rv);
        nullable->null_column_data().emplace_back(0);
    };
    auto append_value_ref = [&](const VariantRowRef& rv) {
        inner_variant->append(rv);
        nullable->null_column_data().emplace_back(0);
    };
    auto append_null = [&]() {
        inner_variant->append_default();
        nullable->null_column_data().emplace_back(1);
        nullable->set_has_null(true);
    };
    auto append_fallback_from_node = [&]() -> bool {
        if (binding.node != nullptr && binding.node->value_column != nullptr) {
            Slice fallback_slice;
            if (ColumnHelper::get_binary_slice_at(binding.node->value_column.get(), row, &fallback_slice)) {
                append_value_ref(
                        VariantRowRef(raw_metadata, std::string_view(fallback_slice.data, fallback_slice.size)));
                return true;
            }
        }
        return false;
    };

    // For scalar paths materialized as VARIANT (typed+fallback heterogeneous),
    // build the field value directly from this node to avoid parsing rebuilt row metadata.
    if (binding.node != nullptr && binding.node->typed_kind == ShreddedTypedKind::SCALAR) {
        const Column* typed_col = nullptr;
        size_t typed_row = 0;
        if (binding.node->typed_value_column != nullptr &&
            ParquetUtils::get_non_null_data_column_and_row(binding.node->typed_value_column.get(), row, &typed_col,
                                                           &typed_row)) {
            auto typed_value =
                    VariantEncoder::encode_datum(typed_col->get(typed_row), *binding.node->typed_value_read_type);
            if (typed_value.ok()) {
                append_value(std::move(typed_value).value());
            } else {
                if (!append_fallback_from_node()) {
                    append_null();
                }
            }
            return;
        }

        if (binding.node->value_column != nullptr) {
            Slice fallback_slice;
            if (ColumnHelper::get_binary_slice_at(binding.node->value_column.get(), row, &fallback_slice)) {
                append_value_ref(
                        VariantRowRef(raw_metadata, std::string_view(fallback_slice.data, fallback_slice.size)));
                return;
            }
        }
        append_null();
        return;
    }

    if (binding.node != nullptr && binding.node->typed_kind == ShreddedTypedKind::ARRAY &&
        binding.node->typed_value_column != nullptr) {
        std::string_view base_array_raw = VariantValue::kEmptyValue;
        if (binding.node->value_column != nullptr) {
            Slice fallback_slice;
            if (ColumnHelper::get_binary_slice_at(binding.node->value_column.get(), row, &fallback_slice)) {
                base_array_raw = std::string_view(fallback_slice.data, fallback_slice.size);
            }
        }
        auto array_overlay = _rebuild_array_overlay(row, *binding.node, raw_metadata, base_array_raw);
        if (array_overlay.ok()) {
            append_value(std::move(array_overlay).value());
            return;
        }
        if (base_array_raw != VariantValue::kEmptyValue) {
            append_value_ref(VariantRowRef(raw_metadata, base_array_raw));
            return;
        }
        append_null();
        return;
    }

    auto parsed_path = VariantPathParser::parse_shredded_path(std::string_view(binding.path));
    if (!parsed_path.ok()) {
        if (append_fallback_from_node()) return;
        append_null();
        return;
    }

    VariantRowRef full_row(built_metadata, built_value);
    auto field = VariantPath::seek_view(full_row, parsed_path.value(), 0);
    if (!field.ok()) {
        VLOG(3) << "variant shredded seek failed for path '" << binding.path << "': " << field.status().to_string()
                << "; falling back";
        if (append_fallback_from_node()) return;
        append_null();
        return;
    }
    append_value_ref(field.value());
}

static bool binding_requires_built_row_seek(const TopBinding& binding) {
    if (binding.kind == TopBinding::Kind::SCALAR) {
        return false;
    }
    if (binding.node == nullptr) {
        return false;
    }
    if (binding.node->typed_kind == ShreddedTypedKind::SCALAR) {
        return false;
    }
    if (binding.node->typed_kind == ShreddedTypedKind::ARRAY && binding.node->typed_value_column != nullptr) {
        return false;
    }
    return true;
}

// Collect all top-row-indexed typed_value_column pointers from the shredded field tree.
// ARRAY node children are element-indexed, not row-indexed, so they are excluded.
// The result is used to build the typed-value presence bitmap in a single column-level pass
// rather than per-row tree traversal, which is more cache-friendly.
static void collect_row_typed_value_columns(const std::vector<ShreddedFieldNode>& nodes,
                                            std::vector<const Column*>* out) {
    for (const auto& node : nodes) {
        if (node.typed_value_column != nullptr) {
            out->push_back(node.typed_value_column.get());
        }
        // Do not recurse into ARRAY children: they use element-level indices, not row indices.
        if (node.typed_kind != ShreddedTypedKind::ARRAY) {
            collect_row_typed_value_columns(node.children, out);
        }
    }
}

// Build a per-row bitmap: bitmap[i] = true when at least one typed_value_column has a
// non-null value at row i.  Uses a column-level scan (cache-friendly) instead of a
// per-row tree traversal, giving O(num_leaf_cols * num_rows) with simple inner loops.
static void build_has_typed_value_bitmap(const std::vector<ShreddedFieldNode>& shredded_fields, size_t num_rows,
                                         std::vector<bool>* bitmap) {
    DCHECK(bitmap != nullptr);
    std::vector<const Column*> typed_cols;
    collect_row_typed_value_columns(shredded_fields, &typed_cols);
    for (const Column* col : typed_cols) {
        if (col == nullptr) continue;
        const Column* data_col = col;
        bool is_const = false;
        if (col->is_constant()) {
            is_const = true;
            data_col = down_cast<const ConstColumn*>(col)->data_column().get();
        }
        if (data_col->is_nullable()) {
            const auto* nullable = down_cast<const NullableColumn*>(data_col);
            if (is_const) {
                // Constant column: single null flag applies to all rows.
                if (!nullable->is_null(0)) {
                    std::fill(bitmap->begin(), bitmap->end(), true);
                    return; // All rows are set; no need to check further columns.
                }
            } else {
                const auto& nulls = nullable->null_column_data();
                for (size_t i = 0; i < num_rows; ++i) {
                    if (!nulls[i]) (*bitmap)[i] = true;
                }
            }
        } else {
            // Non-nullable: every row has a typed value.
            std::fill(bitmap->begin(), bitmap->end(), true);
            return;
        }
    }
}

static void build_row_for_seek(size_t row, std::string_view metadata_raw, std::string_view value_raw,
                               const std::vector<ShreddedFieldNode>& shredded_fields, std::string* out_metadata,
                               std::string* out_value) {
    if (out_metadata == nullptr || out_value == nullptr) {
        return;
    }

    std::vector<VariantBuilder::Overlay> overlays;
    overlays.reserve(16);
    for (const auto& node : shredded_fields) {
        _collect_overlays_for_row(row, metadata_raw, node, overlays);
    }
    if (overlays.empty()) {
        out_metadata->assign(metadata_raw.data(), metadata_raw.size());
        out_value->assign(value_raw.data(), value_raw.size());
        return;
    }

    const bool has_base_payload = !value_raw.empty();
    std::optional<VariantRowRef> base =
            has_base_payload ? std::optional<VariantRowRef>(VariantRowRef(metadata_raw, value_raw)) : std::nullopt;
    auto built = VariantBuilder::build_row_from_overlays(base, std::move(overlays));
    if (!built.ok()) {
        out_metadata->assign(metadata_raw.data(), metadata_raw.size());
        out_value->assign(value_raw.data(), value_raw.size());
        return;
    }

    auto metadata_built = built.value().get_metadata().raw();
    auto value_built = built.value().get_value().raw();
    out_metadata->assign(metadata_built.data(), metadata_built.size());
    out_value->assign(value_built.data(), value_built.size());
}

Status VariantColumnReader::prepare() {
    if (_top_level.metadata_reader == nullptr || _top_level.value_reader == nullptr) {
        return Status::InternalError("Both metadata and value readers are required");
    }
    RETURN_IF_ERROR(_top_level.metadata_reader->prepare());
    RETURN_IF_ERROR(_top_level.value_reader->prepare());
    if (_top_level.root_typed_value_reader != nullptr) {
        RETURN_IF_ERROR(_top_level.root_typed_value_reader->prepare());
    }
    for (auto& node : _shredded_fields) {
        RETURN_IF_ERROR(_prepare_shredded_field_node(&node));
    }
    return Status::OK();
}

void VariantColumnReader::get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) {
    // Only value_reader carries def/rep levels; metadata_reader levels would be dead-written.
    // _top_level.value_reader != nullptr is guaranteed by the constructor DCHECK.
    _top_level.value_reader->get_levels(def_levels, rep_levels, num_levels);
}

static void _set_need_parse_levels_for_shredded_field(ShreddedFieldNode* node, bool need_parse_levels) {
    if (node == nullptr) return;
    if (node->value_reader != nullptr) {
        node->value_reader->set_need_parse_levels(need_parse_levels);
    }
    if (node->typed_value_reader != nullptr) {
        node->typed_value_reader->set_need_parse_levels(need_parse_levels);
    }
    if (node->array_element_value_reader != nullptr) {
        node->array_element_value_reader->set_need_parse_levels(need_parse_levels);
    }
    for (auto& child : node->children) {
        _set_need_parse_levels_for_shredded_field(&child, need_parse_levels);
    }
}

void VariantColumnReader::set_need_parse_levels(bool need_parse_levels) {
    if (_top_level.metadata_reader != nullptr) {
        _top_level.metadata_reader->set_need_parse_levels(need_parse_levels);
    }
    if (_top_level.value_reader != nullptr) {
        _top_level.value_reader->set_need_parse_levels(need_parse_levels);
    }
    if (_top_level.root_typed_value_reader != nullptr) {
        _top_level.root_typed_value_reader->set_need_parse_levels(need_parse_levels);
    }
    for (auto& node : _shredded_fields) {
        _set_need_parse_levels_for_shredded_field(&node, need_parse_levels);
    }
}

void VariantColumnReader::collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                                  int64_t* end_offset, ColumnIOTypeFlags types, bool active) {
    if (_top_level.metadata_reader != nullptr) {
        _top_level.metadata_reader->collect_column_io_range(ranges, end_offset, types, active);
    }
    if (_top_level.value_reader != nullptr) {
        _top_level.value_reader->collect_column_io_range(ranges, end_offset, types, active);
    }
    if (_top_level.root_typed_value_reader != nullptr) {
        _top_level.root_typed_value_reader->collect_column_io_range(ranges, end_offset, types, active);
    }
    for (const auto& node : _shredded_fields) {
        _collect_shredded_field_io_range(node, ranges, end_offset, types, active);
    }
}

void VariantColumnReader::select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) {
    if (_top_level.metadata_reader != nullptr) {
        _top_level.metadata_reader->select_offset_index(range, rg_first_row);
    }
    if (_top_level.value_reader != nullptr) {
        _top_level.value_reader->select_offset_index(range, rg_first_row);
    }
    if (_top_level.root_typed_value_reader != nullptr) {
        _top_level.root_typed_value_reader->select_offset_index(range, rg_first_row);
    }
    for (const auto& node : _shredded_fields) {
        _select_shredded_field_offset_index(node, range, rg_first_row);
    }
}

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
    RETURN_IF_ERROR(_top_level.metadata_reader->read_range(range, filter, metadata_col));
    RETURN_IF_ERROR(_top_level.value_reader->read_range(range, filter, value_col));
    if (_top_level.root_typed_value_reader != nullptr) {
        DCHECK(_top_level.root_typed_value_type != nullptr);
        _top_level.root_typed_value_column = ColumnHelper::create_column(*_top_level.root_typed_value_type, true);
        RETURN_IF_ERROR(
                _top_level.root_typed_value_reader->read_range(range, filter, _top_level.root_typed_value_column));
    } else {
        _top_level.root_typed_value_column = nullptr;
    }
    for (auto& node : _shredded_fields) {
        RETURN_IF_ERROR(_read_shredded_field_node(range, filter, &node));
    }

    auto* metadata_nullable = down_cast<NullableColumn*>(metadata_col->as_mutable_raw_ptr());
    auto* value_nullable = down_cast<NullableColumn*>(value_col->as_mutable_raw_ptr());
    const auto* metadata_column = down_cast<const BinaryColumn*>(metadata_nullable->data_column().get());
    const auto* value_column = down_cast<const BinaryColumn*>(value_nullable->data_column().get());
    const auto& metadata_nulls = metadata_nullable->null_column()->get_data();
    const auto& value_nulls = value_nullable->null_column()->get_data();
    // Verify metadata and value columns are aligned
    DCHECK_EQ(metadata_column->size(), value_column->size());
    DCHECK_EQ(metadata_nulls.size(), value_nulls.size());
    DCHECK_EQ(metadata_nulls.size(), metadata_column->size());

    const size_t num_rows = metadata_column->size();

    // When no explicit paths are requested, auto-discover paths from the shredded field tree.
    // The tree is fixed after construction, so cache the result to avoid repeated traversal.
    if (_shredded_paths.empty() && !_auto_paths_cached) {
        collect_all_top_binding_paths(_shredded_fields, &_cached_auto_paths);
        _auto_paths_cached = true;
    }
    const std::vector<std::string>& effective_paths = _shredded_paths.empty() ? _cached_auto_paths : _shredded_paths;

    std::vector<TopBinding> collected_bindings;
    collect_top_bindings(_shredded_fields, effective_paths, &collected_bindings);
    std::vector<TopBinding> materialized_bindings = select_materialized_bindings(collected_bindings, num_rows);
    const bool request_all_paths = _shredded_paths.empty();

    std::vector<std::string> typed_paths;
    std::vector<TypeDescriptor> typed_types;
    MutableColumns typed_columns;
    typed_paths.reserve(materialized_bindings.size());
    typed_types.reserve(materialized_bindings.size());
    typed_columns.reserve(materialized_bindings.size());
    for (const auto& binding : materialized_bindings) {
        typed_paths.emplace_back(binding.path);
        typed_types.emplace_back(binding.type);
        // One-level only: SCALAR → scalar column, VARIANT → plain VariantColumn (no nested typed_columns).
        typed_columns.emplace_back(ColumnHelper::create_column(binding.type, true));
    }
    variant_column->set_shredded_columns(std::move(typed_paths), std::move(typed_types), std::move(typed_columns),
                                         BinaryColumn::create(), BinaryColumn::create());

    NullColumn reconstructed_null_column(num_rows);
    auto& reconstructed_nulls = reconstructed_null_column.get_data();
    bool has_reconstructed_null = false;
    // Pre-allocate row-build buffers outside the loop to amortize string allocations.
    std::string built_metadata_buf;
    std::string built_value_buf;
    std::string root_typed_metadata_buf;
    std::string root_typed_value_buf;

    // Pre-compute per-row typed-value presence bitmap using a column-level scan.
    // Iterates over each leaf typed_value_column once, marking non-null rows, which is more
    // cache-friendly than the previous per-row tree traversal (O(rows*fields) with tree walk
    // vs O(leaf_cols*rows) with flat null-bitmap scans).
    std::vector<bool> has_typed_value_bitmap(num_rows, false);
    if (!_shredded_fields.empty()) {
        build_has_typed_value_bitmap(_shredded_fields, num_rows, &has_typed_value_bitmap);
    }
    if (_top_level.root_typed_value_column != nullptr) {
        for (size_t i = 0; i < num_rows; ++i) {
            if (has_typed_value_bitmap[i]) {
                continue;
            }
            const Column* root_typed_data = nullptr;
            size_t root_typed_row = 0;
            if (ParquetUtils::get_non_null_data_column_and_row(_top_level.root_typed_value_column.get(), i,
                                                               &root_typed_data, &root_typed_row)) {
                has_typed_value_bitmap[i] = true;
            }
        }
    }

    // Materialize output columns.
    // - request-all-paths: emit rebuilt top-level metadata/value.
    // - requested-subset: keep top-level raw metadata/value and rebuild lazily for bindings that
    //   require full-row seek.
    for (size_t i = 0; i < num_rows; ++i) {
        const bool has_typed_value = has_typed_value_bitmap[i];
        // Iceberg shredded rows may carry payload only in typed_value with base `value` null.
        // Keep those rows non-null so typed overlays can be reconstructed.
        bool is_null = metadata_nulls[i] || (value_nulls[i] && !has_typed_value);
        if (is_null) {
            variant_column->append_shredded_null();
            reconstructed_nulls[i] = 1;
            has_reconstructed_null = true;
            continue;
        }
        const Slice raw_metadata_slice = metadata_column->get_slice(i);
        const Slice raw_value_slice = value_column->get_slice(i);
        if ((raw_metadata_slice.size == 0 || raw_value_slice.size == 0) && !has_typed_value) {
            variant_column->append_shredded_null();
            reconstructed_nulls[i] = 1;
            has_reconstructed_null = true;
            continue;
        }
        std::string_view raw_metadata(raw_metadata_slice.data, raw_metadata_slice.size);
        std::string_view raw_value(raw_value_slice.data, raw_value_slice.size);

        bool use_root_typed_row = false;
        if (_top_level.root_typed_value_column != nullptr) {
            const Column* root_typed_data = nullptr;
            size_t root_typed_row = 0;
            if (ParquetUtils::get_non_null_data_column_and_row(_top_level.root_typed_value_column.get(), i,
                                                               &root_typed_data, &root_typed_row)) {
                DCHECK(_top_level.root_typed_value_type != nullptr);
                auto encoded = VariantEncoder::encode_datum(root_typed_data->get(root_typed_row),
                                                            *_top_level.root_typed_value_type);
                if (encoded.ok()) {
                    auto metadata_raw = encoded.value().get_metadata().raw();
                    auto value_raw = encoded.value().get_value().raw();
                    root_typed_metadata_buf.assign(metadata_raw.data(), metadata_raw.size());
                    root_typed_value_buf.assign(value_raw.data(), value_raw.size());
                    use_root_typed_row = true;
                }
            }
        }

        built_metadata_buf.clear();
        built_value_buf.clear();
        bool built_ready = false;
        auto ensure_built_row = [&]() {
            if (built_ready) {
                return;
            }
            build_row_for_seek(i, raw_metadata, raw_value, _shredded_fields, &built_metadata_buf, &built_value_buf);
            built_ready = true;
        };
        std::string_view row_metadata = raw_metadata;
        std::string_view row_value = raw_value;
        if (use_root_typed_row) {
            row_metadata = std::string_view(root_typed_metadata_buf.data(), root_typed_metadata_buf.size());
            row_value = std::string_view(root_typed_value_buf.data(), root_typed_value_buf.size());
        } else if (request_all_paths) {
            ensure_built_row();
            row_metadata = std::string_view(built_metadata_buf.data(), built_metadata_buf.size());
            row_value = std::string_view(built_value_buf.data(), built_value_buf.size());
            if (row_metadata.empty() || row_value.empty()) {
                variant_column->append_shredded_null();
                reconstructed_nulls[i] = 1;
                has_reconstructed_null = true;
                continue;
            }
        }
        const Slice output_metadata(row_metadata.data(), row_metadata.size());
        const Slice output_value(row_value.data(), row_value.size());
        variant_column->metadata_column()->append_datum(Datum(output_metadata));
        variant_column->remain_value_column()->append_datum(Datum(output_value));

        for (size_t j = 0; j < materialized_bindings.size(); ++j) {
            Column* typed_col_dst = variant_column->mutable_typed_columns()[j].get();
            if (materialized_bindings[j].kind == TopBinding::Kind::SCALAR) {
                append_top_scalar_binding_value(i, materialized_bindings[j], typed_col_dst);
            } else {
                std::string_view built_metadata = row_metadata;
                std::string_view built_value = row_value;
                if (!request_all_paths && binding_requires_built_row_seek(materialized_bindings[j])) {
                    ensure_built_row();
                    built_metadata = std::string_view(built_metadata_buf.data(), built_metadata_buf.size());
                    built_value = std::string_view(built_value_buf.data(), built_value_buf.size());
                }
                append_variant_binding_row_from_built_row(i, materialized_bindings[j], raw_metadata, built_metadata,
                                                          built_value, typed_col_dst);
            }
        }
        reconstructed_nulls[i] = 0;
    }
    DCHECK_EQ(variant_column->size(), num_rows)
            << "Variant column size mismatch: expected " << num_rows << ", got " << variant_column->size();

    // Handle nullable column null flags
    if (dst->is_nullable()) {
        DCHECK(nullable_column != nullptr);
        DCHECK_EQ(variant_column->size(), num_rows)
                << "Variant column size must equal num_rows before setting nullable flags";
        nullable_column->null_column_raw_ptr()->swap_column(reconstructed_null_column);
        nullable_column->set_has_null(has_reconstructed_null);
        DCHECK_EQ(nullable_column->size(), num_rows) << "Final nullable column size mismatch";
    } else {
        // Non-nullable variant column
        DCHECK_EQ(variant_column->size(), num_rows) << "Final variant column size must equal num_rows";
    }

    return Status::OK();
}

} // namespace starrocks::parquet
