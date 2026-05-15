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

#include <fmt/core.h>

#include <algorithm>
#include <optional>
#include <string_view>

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
#include "common/object_pool.h"
#include "exprs/literal.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/schema.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "storage/column_expr_predicate.h"
#include "types/type_info.h"
#include "types/variant_value.h"

namespace starrocks::parquet {

namespace {

TypeDescriptor resolve_virtual_slot_type(VariantColumnReader* source, const VariantPath& leaf_path) {
    if (source == nullptr) {
        return TypeDescriptor(LogicalType::TYPE_UNKNOWN);
    }
    const TypeDescriptor* leaf_type = source->typed_value_read_type_for_path(leaf_path);
    if (leaf_type == nullptr) {
        return TypeDescriptor(LogicalType::TYPE_UNKNOWN);
    }
    return *leaf_type;
}

const ShreddedFieldNode* find_shredded_field_node_for_path(const std::vector<ShreddedFieldNode>& shredded_fields,
                                                           const VariantPath& path) {
    if (path.empty()) return nullptr;
    const std::vector<ShreddedFieldNode>* current = &shredded_fields;
    const ShreddedFieldNode* found_node = nullptr;
    for (size_t i = 0; i < path.segments.size(); ++i) {
        const auto& seg = path.segments[i];
        if (!seg.is_object()) return nullptr;
        found_node = nullptr;
        for (const auto& node : *current) {
            if (node.name == seg.key) {
                found_node = &node;
                break;
            }
        }
        if (found_node == nullptr) return nullptr;
        if (i + 1 < path.segments.size()) {
            current = &found_node->children;
        }
    }
    return found_node;
}

Status rewrite_delegate_predicates(const std::vector<const ColumnPredicate*>& predicates,
                                   const TypeDescriptor& target_type_desc, ObjectPool* pool,
                                   std::vector<const ColumnPredicate*>* rewritten_predicates) {
    DCHECK(pool != nullptr);
    DCHECK(rewritten_predicates != nullptr);

    TypeInfoPtr target_type_info = get_type_info(target_type_desc);
    if (target_type_info == nullptr) {
        return Status::NotSupported(fmt::format("unsupported delegate leaf type: {}", target_type_desc.debug_string()));
    }

    rewritten_predicates->reserve(predicates.size());
    for (const ColumnPredicate* predicate : predicates) {
        if (predicate == nullptr) {
            return Status::InvalidArgument("predicate should not be null");
        }

        const ColumnPredicate* rewritten = nullptr;
        RETURN_IF_ERROR(predicate->convert_to(&rewritten, target_type_info, pool));
        if (rewritten == nullptr) {
            return Status::InternalError("predicate convert_to returned null");
        }
        rewritten_predicates->emplace_back(rewritten);
    }
    return Status::OK();
}

} // namespace

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

// Returns true when the subtree rooted at `node` overlaps with at least one requested path,
// meaning we must recurse into it. Two overlap conditions:
//   1. node is an ancestor-or-equal of a requested path  → must descend to reach the leaf.
//   2. a requested path is an ancestor-or-equal of node  → the request covers this entire subtree.
// When no requested_paths are given (nullptr), all nodes are visited.
static bool _should_read_shredded_field_node(const ShreddedFieldNode& node,
                                             const std::vector<VariantPath>* requested_paths) {
    if (requested_paths == nullptr) {
        return true;
    }
    for (const auto& path : *requested_paths) {
        if (node.parsed_full_path.is_ancestor_or_same(path) || path.is_ancestor_or_same(node.parsed_full_path)) {
            return true;
        }
    }
    return false;
}

// Returns true when the node's own columns (value / typed_value / array_element_value) should
// be read from disk. "Visiting" a node is not the same as reading its columns:
//   - Ancestor nodes must be visited so recursion can reach the requested leaves, but their
//     own columns are skipped unless a requested path points at or above the node itself.
//   - ARRAY nodes are the exception: their offset columns are required by child readers even
//     when the request targets a deeper descendant, so they are always read when visited.
static bool _should_read_shredded_field_node_columns(const ShreddedFieldNode& node,
                                                     const std::vector<VariantPath>* requested_paths) {
    if (requested_paths == nullptr) {
        return true;
    }
    for (const auto& path : *requested_paths) {
        // The requested path covers this node (path is ancestor-or-equal): read columns.
        if (path.is_ancestor_or_same(node.parsed_full_path)) {
            return true;
        }
        // ARRAY node: its offsets are needed by child readers even when the request targets
        // a deeper descendant, so read whenever the node is an ancestor of a requested path.
        if (node.kind == ShreddedFieldNode::Kind::ARRAY && node.parsed_full_path.is_ancestor_or_same(path)) {
            return true;
        }
    }
    return false;
}

static bool _column_chunk_all_null(const ColumnReader* reader) {
    if (reader == nullptr) {
        return false;
    }
    const tparquet::ColumnChunk* chunk_meta = reader->get_chunk_metadata();
    if (chunk_meta == nullptr || !chunk_meta->meta_data.__isset.statistics ||
        !chunk_meta->meta_data.statistics.__isset.null_count) {
        return false;
    }
    return chunk_meta->meta_data.statistics.null_count == chunk_meta->meta_data.num_values;
}

static bool _column_chunk_all_null_for_num_rows(const ColumnReader* reader, uint64_t num_rows) {
    if (reader == nullptr) {
        return false;
    }
    const tparquet::ColumnChunk* chunk_meta = reader->get_chunk_metadata();
    if (chunk_meta == nullptr || !chunk_meta->meta_data.__isset.statistics ||
        !chunk_meta->meta_data.statistics.__isset.null_count) {
        return false;
    }
    return chunk_meta->meta_data.statistics.null_count == static_cast<int64_t>(num_rows);
}

static bool _column_chunk_has_no_null(const ColumnReader* reader) {
    if (reader == nullptr) {
        return false;
    }
    const tparquet::ColumnChunk* chunk_meta = reader->get_chunk_metadata();
    if (chunk_meta == nullptr || !chunk_meta->meta_data.__isset.statistics ||
        !chunk_meta->meta_data.statistics.__isset.null_count) {
        return false;
    }
    return chunk_meta->meta_data.statistics.null_count == 0;
}

static bool _requested_paths_are_scalar_typed_leaves(const std::vector<ShreddedFieldNode>& nodes,
                                                     const std::vector<VariantPath>& requested_paths) {
    if (requested_paths.empty()) {
        return false;
    }
    for (const auto& path : requested_paths) {
        const ShreddedFieldNode* node = find_shredded_field_node_for_path(nodes, path);
        if (node == nullptr || node->kind != ShreddedFieldNode::Kind::SCALAR || node->typed_value_reader == nullptr) {
            return false;
        }
    }
    return true;
}

static bool _requested_scalar_fallback_values_all_null(const std::vector<ShreddedFieldNode>& nodes,
                                                       const std::vector<VariantPath>& requested_paths) {
    for (const auto& path : requested_paths) {
        const ShreddedFieldNode* node = find_shredded_field_node_for_path(nodes, path);
        if (node == nullptr) {
            return false;
        }
        if (node->value_reader != nullptr && !_column_chunk_all_null(node->value_reader.get())) {
            return false;
        }
    }
    return true;
}

static void _clear_shredded_field_node_columns(ShreddedFieldNode* node) {
    if (node == nullptr) {
        return;
    }
    node->value_column = nullptr;
    node->typed_value_column = nullptr;
    node->array_element_value_column = nullptr;
    for (auto& child : node->children) {
        _clear_shredded_field_node_columns(&child);
    }
}

static Status _read_shredded_field_node(const Range<uint64_t>& range, const Filter* filter, ShreddedFieldNode* node,
                                        const std::vector<VariantPath>* requested_paths) {
    if (node == nullptr) {
        return Status::InvalidArgument("node should not be null");
    }
    if (!_should_read_shredded_field_node(*node, requested_paths)) {
        _clear_shredded_field_node_columns(node);
        return Status::OK();
    }
    const bool read_node_columns = _should_read_shredded_field_node_columns(*node, requested_paths);
    // All-null fallback `value` has no usable remain payload for any row in this row group.
    // Keep value_column as nullptr so downstream reconstruction treats it the same as
    // "no fallback payload", while still reading typed_value/children normally.
    if (read_node_columns && node->value_reader != nullptr && !_column_chunk_all_null(node->value_reader.get())) {
        node->value_column = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        RETURN_IF_ERROR(node->value_reader->read_range(range, filter, node->value_column));
    } else {
        node->value_column = nullptr;
    }
    if (read_node_columns && node->typed_value_reader != nullptr) {
        node->typed_value_column = ColumnHelper::create_column(*node->typed_value_read_type, true);
        RETURN_IF_ERROR(node->typed_value_reader->read_range(range, filter, node->typed_value_column));
    } else {
        node->typed_value_column = nullptr;
    }
    if (read_node_columns && node->array_element_value_reader != nullptr) {
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
        RETURN_IF_ERROR(_read_shredded_field_node(range, filter, &child, requested_paths));
    }
    return Status::OK();
}

static void _collect_shredded_field_io_range(const ShreddedFieldNode& node,
                                             std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                             int64_t* end_offset, ColumnIOTypeFlags types, bool active,
                                             const std::vector<VariantPath>* requested_paths) {
    if (!_should_read_shredded_field_node(node, requested_paths)) {
        return;
    }
    const bool read_node_columns = _should_read_shredded_field_node_columns(node, requested_paths);
    if (read_node_columns && node.value_reader != nullptr && !_column_chunk_all_null(node.value_reader.get())) {
        node.value_reader->collect_column_io_range(ranges, end_offset, types, active);
    }
    if (read_node_columns && node.typed_value_reader != nullptr) {
        node.typed_value_reader->collect_column_io_range(ranges, end_offset, types, active);
    }
    if (read_node_columns && node.array_element_value_reader != nullptr) {
        node.array_element_value_reader->collect_column_io_range(ranges, end_offset, types, active);
    }
    for (const auto& child : node.children) {
        _collect_shredded_field_io_range(child, ranges, end_offset, types, active, requested_paths);
    }
}

static void _select_shredded_field_offset_index(const ShreddedFieldNode& node, const SparseRange<uint64_t>& range,
                                                const uint64_t rg_first_row,
                                                const std::vector<VariantPath>* requested_paths) {
    if (!_should_read_shredded_field_node(node, requested_paths)) {
        return;
    }
    const bool read_node_columns = _should_read_shredded_field_node_columns(node, requested_paths);
    if (read_node_columns && node.value_reader != nullptr && !_column_chunk_all_null(node.value_reader.get())) {
        node.value_reader->select_offset_index(range, rg_first_row);
    }
    if (read_node_columns && node.typed_value_reader != nullptr) {
        node.typed_value_reader->select_offset_index(range, rg_first_row);
    }
    if (read_node_columns && node.array_element_value_reader != nullptr) {
        node.array_element_value_reader->select_offset_index(range, rg_first_row);
    }
    for (const auto& child : node.children) {
        _select_shredded_field_offset_index(child, range, rg_first_row, requested_paths);
    }
}

// Maximum nesting depth for mutually-recursive array overlay reconstruction.
// Prevents stack-overflow on pathological / malformed Parquet files where
// typed_value fields nest arrays inside arrays beyond reasonable depth.
static constexpr int kMaxShreddedArrayNestingDepth = 32;

// Returns the first non-null typed_value_column found in the node subtree, or nullptr.
// Used to derive num_rows when base payload is skipped.
static const Column* _find_first_typed_col(const ShreddedFieldNode& node) {
    if (node.typed_value_column != nullptr) return node.typed_value_column.get();
    for (const auto& child : node.children) {
        if (const Column* c = _find_first_typed_col(child); c != nullptr) return c;
    }
    return nullptr;
}

// Collect overlays for one ARRAY element row from shredded child nodes recursively.
// Priority per path: typed scalar value > fallback binary value.
// Failures while encoding/rebuilding typed payloads are treated as hard errors to avoid silently
// dropping shredded data.
static Status _collect_overlays_for_array_element(size_t element_row, const std::vector<ShreddedFieldNode>& nodes,
                                                  std::string_view metadata_raw,
                                                  std::vector<VariantBuilder::Overlay>* overlays, int depth = 0);

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
static StatusOr<std::optional<VariantRowValue>> _rebuild_array_overlay(size_t row, const ShreddedFieldNode& array_node,
                                                                       std::string_view metadata_raw,
                                                                       std::string_view base_array_raw, int depth = 0) {
    if (depth > kMaxShreddedArrayNestingDepth) {
        LOG(WARNING) << "variant shredded array nesting depth exceeded limit (" << kMaxShreddedArrayNestingDepth
                     << ") at path='" << array_node.full_path << "'";
        return Status::ResourceBusy("shredded array nesting depth limit exceeded");
    }
    const Column* typed_col = nullptr;
    size_t typed_row = 0;
    if (!ParquetUtils::get_non_null_data_column_and_row(array_node.typed_value_column.get(), row, &typed_col,
                                                        &typed_row) ||
        !typed_col->is_array()) {
        if (base_array_raw != VariantValue::kEmptyValue) {
            return std::optional<VariantRowValue>(VariantRowValue(metadata_raw, base_array_raw));
        }
        return std::nullopt;
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
                RETURN_IF_ERROR(_collect_overlays_for_array_element(typed_begin + i, array_node.children, metadata_raw,
                                                                    &element_overlays, depth + 1));
            }

            if (!element_overlays.empty() || base_element.has_value()) {
                auto built = VariantBuilder::build_row_from_overlays(base_element, std::move(element_overlays));
                if (!built.ok()) {
                    return built.status().clone_and_prepend(
                            strings::Substitute("rebuild shredded array object element failed, path=$0, index=$1",
                                                array_node.full_path, i));
                }
                array_builder.add(std::move(built).value());
                continue;
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
                return element_value.status().clone_and_prepend(strings::Substitute(
                        "encode shredded array scalar element failed, path=$0, index=$1", array_node.full_path, i));
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
    ASSIGN_OR_RETURN(auto built, array_builder.build());
    return std::optional<VariantRowValue>(std::move(built));
}

// Recursive worker used by ARRAY reconstruction to collect per-element overlays.
// It walks child shredded nodes, preferring typed scalar data and using remain-binary slices only when
// typed data is absent for the current row/element.
static Status _collect_overlays_for_array_element(size_t element_row, const std::vector<ShreddedFieldNode>& nodes,
                                                  std::string_view metadata_raw,
                                                  std::vector<VariantBuilder::Overlay>* overlays, int depth) {
    if (overlays == nullptr) {
        return Status::InvalidArgument("variant element overlays output is null");
    }
    if (depth > kMaxShreddedArrayNestingDepth) {
        return Status::ResourceBusy(strings::Substitute(
                "variant shredded array element nesting depth exceeded limit ($0)", kMaxShreddedArrayNestingDepth));
    }
    for (const auto& node : nodes) {
        if (node.kind == ShreddedFieldNode::Kind::SCALAR && node.typed_value_column != nullptr) {
            const Column* typed_col = nullptr;
            size_t typed_row = 0;
            if (ParquetUtils::get_non_null_data_column_and_row(node.typed_value_column.get(), element_row, &typed_col,
                                                               &typed_row)) {
                auto typed_value = VariantEncoder::encode_datum(typed_col->get(typed_row), *node.typed_value_read_type);
                if (!typed_value.ok()) {
                    return typed_value.status().clone_and_prepend(strings::Substitute(
                            "encode shredded array element scalar failed, path=$0", node.full_path));
                }
                overlays->emplace_back(VariantBuilder::Overlay{.path = node.parsed_full_path,
                                                               .value = std::move(typed_value).value()});
                continue;
            }
        }

        if (node.kind == ShreddedFieldNode::Kind::ARRAY && node.typed_value_column != nullptr) {
            // Nested array sub-field within an array element (e.g., array-of-objects where
            // one field is itself a shredded array). Reconstruct recursively.
            Slice fallback_slice;
            std::string_view base_array_raw = VariantValue::kEmptyValue;
            if (ColumnHelper::get_binary_slice_at(node.value_column.get(), element_row, &fallback_slice)) {
                base_array_raw = std::string_view(fallback_slice.data, fallback_slice.size);
            }
            auto array_overlay = _rebuild_array_overlay(element_row, node, metadata_raw, base_array_raw, depth + 1);
            if (!array_overlay.ok()) {
                return array_overlay.status().clone_and_prepend(
                        strings::Substitute("rebuild shredded array element failed, path=$0", node.full_path));
            }
            if (array_overlay.value().has_value()) {
                overlays->emplace_back(VariantBuilder::Overlay{.path = node.parsed_full_path,
                                                               .value = std::move(*array_overlay.value())});
            }
            continue;
        }

        // NONE node (or SCALAR/ARRAY with no typed data): emit fallback binary then recurse children.
        Slice fallback_slice;
        if (ColumnHelper::get_binary_slice_at(node.value_column.get(), element_row, &fallback_slice)) {
            overlays->emplace_back(VariantBuilder::Overlay{
                    .path = node.parsed_full_path,
                    .value = VariantRowValue(std::string_view(metadata_raw),
                                             std::string_view(fallback_slice.data, fallback_slice.size))});
        }
        if (!node.children.empty()) {
            RETURN_IF_ERROR(
                    _collect_overlays_for_array_element(element_row, node.children, metadata_raw, overlays, depth + 1));
        }
    }
    return Status::OK();
}

static StatusOr<VariantPath> make_relative_variant_path(const VariantPath& full_path, size_t prefix_segments);

StatusOr<std::optional<VariantRowValue>> VariantColumnReader::build_variant_binding_from_node(
        size_t row, const ShreddedFieldNode& node, std::string_view metadata_raw) {
    std::optional<VariantRowRef> base;
    if (node.value_column != nullptr) {
        Slice fallback_slice;
        if (ColumnHelper::get_binary_slice_at(node.value_column.get(), row, &fallback_slice)) {
            base.emplace(metadata_raw, std::string_view(fallback_slice.data, fallback_slice.size));
        }
    }

    if (node.kind == ShreddedFieldNode::Kind::SCALAR && node.typed_value_column != nullptr) {
        const Column* typed_col = nullptr;
        size_t typed_row = 0;
        if (ParquetUtils::get_non_null_data_column_and_row(node.typed_value_column.get(), row, &typed_col,
                                                           &typed_row)) {
            auto typed_value = VariantEncoder::encode_datum(typed_col->get(typed_row), *node.typed_value_read_type);
            if (!typed_value.ok()) {
                return typed_value.status().clone_and_prepend(
                        strings::Substitute("encode shredded scalar failed, path=$0", node.full_path));
            }
            return std::optional<VariantRowValue>(std::move(typed_value).value());
        }
        if (base.has_value()) {
            auto owned_base = base->to_owned();
            return std::optional<VariantRowValue>(std::move(owned_base));
        }
        return std::optional<VariantRowValue>();
    }

    if (node.kind == ShreddedFieldNode::Kind::ARRAY && node.typed_value_column != nullptr) {
        std::string_view base_array_raw = VariantValue::kEmptyValue;
        if (base.has_value()) {
            base_array_raw = base->get_value().raw();
        }
        auto array_overlay = _rebuild_array_overlay(row, node, metadata_raw, base_array_raw);
        if (!array_overlay.ok()) {
            return array_overlay.status().clone_and_prepend(
                    strings::Substitute("rebuild shredded array failed, path=$0", node.full_path));
        }
        return array_overlay;
    }

    if (node.children.empty()) {
        if (base.has_value()) {
            auto owned_base = base->to_owned();
            return std::optional<VariantRowValue>(std::move(owned_base));
        }
        return std::optional<VariantRowValue>();
    }

    std::vector<VariantBuilder::Overlay> overlays;
    overlays.reserve(node.children.size() * 2);
    const size_t prefix_segments = node.parsed_full_path.segments.size();
    for (const auto& child : node.children) {
        auto child_value = VariantColumnReader::build_variant_binding_from_node(row, child, metadata_raw);
        if (!child_value.ok()) {
            return child_value.status();
        }
        if (child_value->has_value()) {
            ASSIGN_OR_RETURN(auto relative_path, make_relative_variant_path(child.parsed_full_path, prefix_segments));
            overlays.emplace_back(
                    VariantBuilder::Overlay{.path = std::move(relative_path), .value = std::move(**child_value)});
        }
    }

    if (overlays.empty()) {
        if (base.has_value()) {
            auto owned_base = base->to_owned();
            return std::optional<VariantRowValue>(std::move(owned_base));
        }
        return std::optional<VariantRowValue>();
    }

    ASSIGN_OR_RETURN(auto built, VariantBuilder::build_row_from_overlays(base, std::move(overlays)));
    return std::optional<VariantRowValue>(std::move(built));
}

// Auto-discover binding paths from the shredded_fields tree when no explicit shredded_paths are
// provided.  Stops at ARRAY boundaries (does not recurse into array element children) and at SCALAR
// leaves.  Struct-like NONE nodes are recursed.
static void collect_all_top_binding_paths(const std::vector<ShreddedFieldNode>& nodes,
                                          std::vector<VariantPath>* paths) {
    for (const auto& node : nodes) {
        if (node.kind != ShreddedFieldNode::Kind::NONE) {
            // SCALAR leaf or ARRAY boundary — emit and stop recursing.
            paths->push_back(node.parsed_full_path);
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
//
// Accepts pre-parsed VariantPath objects directly to avoid string→VariantPath round-trips.
// Callers must not pass string-form paths.
static void collect_top_bindings(const std::vector<ShreddedFieldNode>& nodes,
                                 const std::vector<VariantPath>& shredded_paths, std::vector<TopBinding>* out) {
    if (out == nullptr) {
        return;
    }
    // For unshredded columns (no shredded nodes), skip binding collection entirely.
    // Bindings with node==nullptr cause a per-row seek+encode in append_variant_binding_row
    // that writes sub-variants into typed_columns, only for _fill_dst_chunk to read them back
    // and seek again.  For unshredded files, _fill_dst_chunk/_seek_base navigates the path
    // directly from the base payload in one pass, which is both simpler and faster.
    if (nodes.empty()) {
        return;
    }
    std::vector<VariantPath> auto_paths;
    const std::vector<VariantPath>* effective_paths = &shredded_paths;
    if (shredded_paths.empty()) {
        collect_all_top_binding_paths(nodes, &auto_paths);
        effective_paths = &auto_paths;
        if (effective_paths->empty()) return;
    }
    for (const auto& path : *effective_paths) {
        // Derive the canonical string form for binding.path (used in error messages).
        // Paths in shredded_paths are object-segment-only (guaranteed by add_path validation),
        // so to_shredded_path() always succeeds here.
        auto path_str_opt = path.to_shredded_path();
        DCHECK(path_str_opt.has_value()) << "shredded path must not contain array segments";
        std::string path_str = path_str_opt.value_or("");

        const ShreddedFieldNode* node = find_shredded_field_node_for_path(nodes, path);
        if (node == nullptr) {
            // Path requested but not shredded in this file/RG: keep requested typed path.
            out->push_back({.kind = TopBinding::Kind::VARIANT,
                            .path = std::move(path_str),
                            .type = variant_type_desc(),
                            .node = nullptr,
                            .parsed_path = path});
            continue;
        }
        if (node->kind == ShreddedFieldNode::Kind::SCALAR && node->typed_value_column != nullptr) {
            out->push_back({.kind = TopBinding::Kind::SCALAR,
                            .path = std::move(path_str),
                            .type = *node->typed_value_read_type,
                            .node = node,
                            .parsed_path = path});
        } else {
            // Array boundary / struct children / fallback-only: pack as plain VariantColumn.
            out->push_back({.kind = TopBinding::Kind::VARIANT,
                            .path = std::move(path_str),
                            .type = variant_type_desc(),
                            .node = node,
                            .parsed_path = path});
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
            VariantScalarMaterializeMode mode =
                    VariantColumnReader::decide_variant_scalar_materialize_mode(binding.node, num_rows);
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

VariantScalarMaterializeMode VariantColumnReader::decide_variant_scalar_materialize_mode(const ShreddedFieldNode* node,
                                                                                         size_t num_rows) {
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

static StatusOr<VariantPath> make_relative_variant_path(const VariantPath& full_path, size_t prefix_segments) {
    if (full_path.segments.size() < prefix_segments) {
        return Status::InternalError("variant overlay path shorter than subtree prefix");
    }
    std::vector<VariantSegment> relative_segments;
    relative_segments.reserve(full_path.segments.size() - prefix_segments);
    for (size_t i = prefix_segments; i < full_path.segments.size(); ++i) {
        relative_segments.emplace_back(full_path.segments[i]);
    }
    return VariantPath(std::move(relative_segments));
}

// Append one row of a VARIANT binding to dst (a NullableColumn<VariantColumn>).
// For shredded bindings, materializes the requested subtree directly from that node.
// For non-shredded bindings (node == nullptr), seeks the requested path from the current row payload.
// dst is kept in object mode (VariantRowValue per entry); no nested typed_columns inside.
Status VariantColumnReader::append_variant_binding_row(size_t row, const TopBinding& binding,
                                                       std::string_view raw_metadata, const VariantRowRef& full_row,
                                                       Column* dst) {
    if (dst == nullptr) return Status::OK();
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

    if (binding.node != nullptr) {
        auto value = VariantColumnReader::build_variant_binding_from_node(row, *binding.node, raw_metadata);
        if (!value.ok()) {
            return value.status().clone_and_prepend(
                    strings::Substitute("build shredded variant binding failed, path=$0", binding.path));
        }
        if (!value->has_value()) {
            append_null();
            return Status::OK();
        }
        append_value(**value);
        return Status::OK();
    }

    // Use the pre-parsed path cached in binding (parsed once at binding-build time).
    // Invariant: a non-empty path must have a non-empty parsed_path; if violated the seek
    // would silently return the whole variant root instead of the intended sub-field.
    DCHECK(binding.path.empty() || !binding.parsed_path.segments.empty())
            << "TopBinding has non-empty path but empty parsed_path: path=" << binding.path;
    auto field = VariantPath::seek_view(full_row, binding.parsed_path, 0);
    if (!field.ok()) {
        // Path not found (e.g. type mismatch at intermediate node): treat as missing.
        append_null();
        return Status::OK();
    }
    // Field found — append even if the value is JSON null (basic_type=Null).
    append_value_ref(field.value());
    return Status::OK();
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
        if (node.kind != ShreddedFieldNode::Kind::ARRAY) {
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

class VariantReadRangeBatchContext {
public:
    VariantReadRangeBatchContext(const std::vector<ShreddedFieldNode>& shredded_fields,
                                 const std::vector<TopBinding>& materialized_bindings,
                                 const Column* root_typed_value_column, const TypeDescriptor* root_typed_value_type,
                                 const BinaryColumn* metadata_column, const BinaryColumn* value_column,
                                 ImmutableNullData metadata_nulls, ImmutableNullData value_nulls)
            : materialized_bindings(materialized_bindings),
              root_typed_value_column(root_typed_value_column),
              root_typed_value_type(root_typed_value_type),
              metadata_column(metadata_column),
              value_column(value_column),
              metadata_nulls(metadata_nulls),
              value_nulls(value_nulls),
              has_typed_value_bitmap(metadata_column != nullptr ? metadata_column->size() : 0, false) {
        const size_t num_rows = has_typed_value_bitmap.size();
        if (!shredded_fields.empty()) {
            build_has_typed_value_bitmap(shredded_fields, num_rows, &has_typed_value_bitmap);
        }
        if (root_typed_value_column != nullptr) {
            for (size_t i = 0; i < num_rows; ++i) {
                if (has_typed_value_bitmap[i]) {
                    continue;
                }
                const Column* root_typed_data = nullptr;
                size_t root_typed_row = 0;
                if (ParquetUtils::get_non_null_data_column_and_row(root_typed_value_column, i, &root_typed_data,
                                                                   &root_typed_row)) {
                    has_typed_value_bitmap[i] = true;
                }
            }
        }
    }

    // Final output bindings after applying requested paths and per-batch materialization
    // decisions. These drive typed_paths/typed_columns layout in the result VariantColumn.
    const std::vector<TopBinding>& materialized_bindings;
    // Optional row-indexed top-level typed_value column for non-STRUCT root typed_value.
    const Column* root_typed_value_column;
    // Type descriptor paired with root_typed_value_column. Needed when encoding the root typed
    // datum back into canonical Variant metadata/value bytes.
    const TypeDescriptor* root_typed_value_type;
    // Base top-level variant payload columns from the file.
    const BinaryColumn* metadata_column;
    const BinaryColumn* value_column;
    // Null flags paired with the base payload columns above.
    ImmutableNullData metadata_nulls;
    ImmutableNullData value_nulls;
    // Per-row "any typed payload exists" summary over shredded fields plus root typed_value.
    // Used only for top-level row null/materialization decisions.
    std::vector<bool> has_typed_value_bitmap;

    bool can_bulk_append_base_payload(bool has_outer_null_channel) const {
        if (root_typed_value_column != nullptr) {
            return false;
        }
        if (has_outer_null_channel) {
            return true;
        }
        const size_t num_rows = has_typed_value_bitmap.size();
        for (size_t i = 0; i < num_rows; ++i) {
            const bool has_typed_value = has_typed_value_bitmap[i];
            if (metadata_nulls[i] || (value_nulls[i] && !has_typed_value)) {
                return false;
            }
            const Slice raw_metadata_slice = metadata_column->get_slice(i);
            const Slice raw_value_slice = value_column->get_slice(i);
            if ((raw_metadata_slice.size == 0 || raw_value_slice.size == 0) && !has_typed_value) {
                return false;
            }
        }
        return true;
    }
};

static void append_null_to_typed_bindings(VariantColumn* variant_column) {
    DCHECK(variant_column != nullptr);
    for (auto& typed_column : variant_column->mutable_typed_columns()) {
        typed_column->append_nulls(1);
    }
}

class VariantReadRangeRowMaterializer {
public:
    VariantReadRangeRowMaterializer(const VariantReadRangeBatchContext& batch_ctx, size_t row,
                                    VariantColumn* variant_column)
            : _batch_ctx(batch_ctx), _row(row), _variant_column(variant_column) {}

    void set_row(size_t row) { _row = row; }

    StatusOr<bool> prepare() {
        const bool has_typed_value = _batch_ctx.has_typed_value_bitmap[_row];
        // Iceberg shredded rows may carry payload only in typed_value with base `value` null.
        if (_batch_ctx.metadata_nulls[_row] || (_batch_ctx.value_nulls[_row] && !has_typed_value)) {
            return false;
        }

        const Slice raw_metadata_slice = _batch_ctx.metadata_column->get_slice(_row);
        const Slice raw_value_slice = _batch_ctx.value_column->get_slice(_row);
        if ((raw_metadata_slice.size == 0 || raw_value_slice.size == 0) && !has_typed_value) {
            return false;
        }

        _raw_metadata = std::string_view(raw_metadata_slice.data, raw_metadata_slice.size);
        _raw_value = std::string_view(raw_value_slice.data, raw_value_slice.size);
        _row_metadata = _raw_metadata;
        _row_value = _raw_value;

        ASSIGN_OR_RETURN(bool use_root_typed_row, _try_use_root_typed_row());
        if (use_root_typed_row) {
            return true;
        }

        return true;
    }

    void append_top_level_row() const {
        DCHECK(_variant_column != nullptr);
        const Slice output_metadata(_row_metadata.data(), _row_metadata.size());
        const Slice output_value(_row_value.data(), _row_value.size());
        _variant_column->metadata_column()->append_datum(Datum(output_metadata));
        _variant_column->remain_value_column()->append_datum(Datum(output_value));
    }

    // append_bindings processes all materialized bindings for the current row.
    Status append_bindings() const {
        for (size_t i = 0; i < _batch_ctx.materialized_bindings.size(); ++i) {
            const auto& binding = _batch_ctx.materialized_bindings[i];
            Column* typed_col_dst = _variant_column->mutable_typed_columns()[i].get();
            if (binding.kind == TopBinding::Kind::SCALAR) {
                append_top_scalar_binding_value(_row, binding, typed_col_dst);
            } else {
                // When the base value column is null (fully-shredded row), _row_value is an
                // empty string_view which is not a valid VariantValue.  Use the default null
                // VariantRowRef so that append_variant_binding_row treats any path lookup on
                // a missing full-row as null rather than crashing.
                VariantRowRef full_row =
                        _row_value.empty() ? VariantRowRef() : VariantRowRef(_row_metadata, _row_value);
                RETURN_IF_ERROR(VariantColumnReader::append_variant_binding_row(_row, binding, _raw_metadata, full_row,
                                                                                typed_col_dst));
            }
        }
        return Status::OK();
    }

private:
    StatusOr<bool> _try_use_root_typed_row() {
        if (_batch_ctx.root_typed_value_column == nullptr) {
            return false;
        }

        const Column* root_typed_data = nullptr;
        size_t root_typed_row = 0;
        if (!ParquetUtils::get_non_null_data_column_and_row(_batch_ctx.root_typed_value_column, _row, &root_typed_data,
                                                            &root_typed_row)) {
            return false;
        }

        DCHECK(_batch_ctx.root_typed_value_type != nullptr);
        ASSIGN_OR_RETURN(auto encoded, VariantEncoder::encode_datum(root_typed_data->get(root_typed_row),
                                                                    *_batch_ctx.root_typed_value_type));

        auto metadata_raw = encoded.get_metadata().raw();
        auto value_raw = encoded.get_value().raw();
        _root_typed_metadata_buf.assign(metadata_raw.data(), metadata_raw.size());
        _root_typed_value_buf.assign(value_raw.data(), value_raw.size());
        _row_metadata = std::string_view(_root_typed_metadata_buf.data(), _root_typed_metadata_buf.size());
        _row_value = std::string_view(_root_typed_value_buf.data(), _root_typed_value_buf.size());
        return true;
    }

    const VariantReadRangeBatchContext& _batch_ctx;
    size_t _row;
    VariantColumn* _variant_column;

    std::string_view _raw_metadata;
    std::string_view _raw_value;
    std::string_view _row_metadata;
    std::string_view _row_value;
    std::string _root_typed_metadata_buf;
    std::string _root_typed_value_buf;
};

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
    // Determine whether base payload reads can be optimised.
    //
    // When every explicitly requested path resolves to a SCALAR typed_value node and there is
    // no root-level typed_value re-encoding, the metadata column is never needed (it is only
    // used for variant binary decoding).  We set _skip_base_payload = true in this case.
    //
    // Top-level value is only needed if leaf fallback values force us back to full
    // per-row reconstruction. For nullable variant columns, top-level metadata is only
    // decoded when statistics cannot prove the outer null mask is all zero.
    _skip_base_payload = false;
    if (!_requested_shredded_paths.empty() && _top_level.root_typed_value_reader == nullptr) {
        _skip_base_payload = _requested_paths_are_scalar_typed_leaves(_shredded_fields, _requested_shredded_paths);
    }
    return Status::OK();
}

VariantVirtualZoneMapReader::VariantVirtualZoneMapReader(VariantColumnReader* source, VariantPath leaf_path)
        : VariantVirtualZoneMapReader(source, leaf_path, resolve_virtual_slot_type(source, leaf_path)) {}

VariantVirtualZoneMapReader::VariantVirtualZoneMapReader(VariantColumnReader* source, VariantPath leaf_path,
                                                         TypeDescriptor virtual_slot_type)
        : ColumnReader(nullptr),
          _source(source),
          _leaf_path(std::move(leaf_path)),
          _virtual_slot_type(std::move(virtual_slot_type)) {}

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
    const TopLevelSkipFlags skip_flags = _compute_top_level_skip_flags();

    if (_top_level.metadata_reader != nullptr && !skip_flags.skip_metadata) {
        _top_level.metadata_reader->collect_column_io_range(ranges, end_offset, types, active);
    }
    if (_top_level.value_reader != nullptr && !skip_flags.skip_payload) {
        _top_level.value_reader->collect_column_io_range(ranges, end_offset, types, active);
    }
    if (_top_level.root_typed_value_reader != nullptr) {
        _top_level.root_typed_value_reader->collect_column_io_range(ranges, end_offset, types, active);
    }
    const std::vector<VariantPath>* requested_paths =
            _requested_shredded_paths.empty() ? nullptr : &_requested_shredded_paths;
    for (const auto& node : _shredded_fields) {
        _collect_shredded_field_io_range(node, ranges, end_offset, types, active, requested_paths);
    }
}

void VariantColumnReader::select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) {
    const TopLevelSkipFlags skip_flags = _compute_top_level_skip_flags();

    if (_top_level.metadata_reader != nullptr && !skip_flags.skip_metadata) {
        _top_level.metadata_reader->select_offset_index(range, rg_first_row);
    }
    if (_top_level.value_reader != nullptr && !skip_flags.skip_payload) {
        _top_level.value_reader->select_offset_index(range, rg_first_row);
    }
    if (_top_level.root_typed_value_reader != nullptr) {
        _top_level.root_typed_value_reader->select_offset_index(range, rg_first_row);
    }
    const std::vector<VariantPath>* requested_paths =
            _requested_shredded_paths.empty() ? nullptr : &_requested_shredded_paths;
    for (const auto& node : _shredded_fields) {
        _select_shredded_field_offset_index(node, range, rg_first_row, requested_paths);
    }
}

VariantColumnReader::TopLevelSkipFlags VariantColumnReader::_compute_top_level_skip_flags() const {
    TopLevelSkipFlags flags;
    flags.skip_payload = _top_level.root_typed_value_reader == nullptr &&
                         _requested_paths_are_scalar_typed_leaves(_shredded_fields, _requested_shredded_paths) &&
                         _requested_scalar_fallback_values_all_null(_shredded_fields, _requested_shredded_paths);
    flags.skip_metadata = flags.skip_payload && _column_chunk_has_no_null(_top_level.metadata_reader.get());
    return flags;
}

// Fast-path read when _skip_base_payload is true.
// Reads shredded fields, then calls select_materialized_bindings to detect per-field fallback
// rows (type mismatch → row value lives in node->value_column instead of typed_value_column).
// Returns true  → fast path handled everything; caller is done.
// Returns false → fallback rows detected; shredded fields are already populated and the caller
//                 must run the normal per-row path without re-reading shredded fields.
StatusOr<bool> VariantColumnReader::_read_range_skip_base_payload(const Range<uint64_t>& range, const Filter* filter,
                                                                  VariantColumn* variant_column,
                                                                  NullableColumn* nullable_column) {
    _top_level.root_typed_value_column = nullptr;
    const std::vector<VariantPath>* requested_paths = &_requested_shredded_paths;
    for (auto& node : _shredded_fields) {
        RETURN_IF_ERROR(_read_shredded_field_node(range, filter, &node, requested_paths));
    }

    // Derive num_rows from the first available typed_value_column.
    size_t num_rows = 0;
    for (const auto& node : _shredded_fields) {
        if (const Column* c = _find_first_typed_col(node); c != nullptr) {
            num_rows = c->size();
            break;
        }
    }

    std::vector<TopBinding> collected_bindings;
    collect_top_bindings(_shredded_fields, _requested_shredded_paths, &collected_bindings);

    // If any binding is demoted to VARIANT (type mismatch), signal the caller to use the normal
    // per-row path. Shredded fields are already populated so the caller skips re-reading them.
    std::vector<TopBinding> materialized_bindings = select_materialized_bindings(collected_bindings, num_rows);
    for (const auto& b : materialized_bindings) {
        if (b.kind == TopBinding::Kind::VARIANT) {
            return false;
        }
    }

    // No fallback rows: bulk-copy typed_value columns directly.
    std::vector<std::string> typed_paths;
    std::vector<TypeDescriptor> typed_types;
    MutableColumns typed_columns;
    typed_paths.reserve(collected_bindings.size());
    typed_types.reserve(collected_bindings.size());
    typed_columns.reserve(collected_bindings.size());
    for (const auto& binding : collected_bindings) {
        typed_paths.emplace_back(binding.path);
        typed_types.emplace_back(binding.type);
        typed_columns.emplace_back(ColumnHelper::create_column(binding.type, true));
    }
    variant_column->set_shredded_columns(std::move(typed_paths), std::move(typed_types), std::move(typed_columns),
                                         nullptr, nullptr);

    for (size_t bi = 0; bi < collected_bindings.size(); ++bi) {
        const TopBinding& binding = collected_bindings[bi];
        const Column* src = binding.node != nullptr ? binding.node->typed_value_column.get() : nullptr;
        Column* col_dst = variant_column->mutable_typed_columns()[bi].get();
        if (src == nullptr) {
            col_dst->append_nulls(num_rows);
            continue;
        }
        DCHECK_EQ(src->size(), num_rows) << "typed_value_column size mismatch for '" << binding.path << "': expected "
                                         << num_rows << ", got " << src->size();
        if (src->size() != num_rows) {
            col_dst->append_nulls(num_rows);
            continue;
        }
        col_dst->append(*src, 0, num_rows);
    }
    DCHECK_EQ(variant_column->size(), num_rows);

    // Outer row-null mask from metadata column: metadata null ↔ variant row is null (shredding spec).
    // The value column is null for both "variant is null" and "all fields fully shredded", so it
    // cannot be used as a reliable null indicator here.
    if (nullable_column != nullptr) {
        DCHECK(_top_level.metadata_reader != nullptr);
        auto* outer_null = nullable_column->null_column_raw_ptr();
        if (_column_chunk_has_no_null(_top_level.metadata_reader.get())) {
            outer_null->get_data().assign(num_rows, 0);
            nullable_column->set_has_null(false);
        } else {
            ColumnPtr metadata_col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
            RETURN_IF_ERROR(_top_level.metadata_reader->read_range(range, filter, metadata_col));
            const auto* metadata_nullable = down_cast<const NullableColumn*>(metadata_col.get());
            DCHECK_EQ(metadata_nullable->size(), num_rows);
            outer_null->get_data().assign(metadata_nullable->null_column()->get_data().begin(),
                                          metadata_nullable->null_column()->get_data().end());
            nullable_column->set_has_null(metadata_nullable->has_null());
        }
    }
    return true;
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

    // Fast path: all requested paths are SCALAR typed_value leaves and base payload is not needed.
    // If the fast path detects per-field fallback rows it returns false; shredded fields are
    // already populated so we fall through to the normal path skipping shredded re-reads.
    bool shredded_already_read = false;
    if (_skip_base_payload) {
        ASSIGN_OR_RETURN(bool handled, _read_range_skip_base_payload(range, filter, variant_column, nullable_column));
        if (handled) {
            return Status::OK();
        }
        shredded_already_read = true;
    }

    ColumnPtr metadata_col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    ColumnPtr value_col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    RETURN_IF_ERROR(_top_level.metadata_reader->read_range(range, filter, metadata_col));
    RETURN_IF_ERROR(_top_level.value_reader->read_range(range, filter, value_col));
    // root_typed_value_reader is always nullptr when _skip_base_payload is true, so this branch
    // is only reached from the non-skip path.
    if (_top_level.root_typed_value_reader != nullptr) {
        DCHECK(!_skip_base_payload);
        DCHECK(_top_level.root_typed_value_type != nullptr);
        _top_level.root_typed_value_column = ColumnHelper::create_column(*_top_level.root_typed_value_type, true);
        RETURN_IF_ERROR(
                _top_level.root_typed_value_reader->read_range(range, filter, _top_level.root_typed_value_column));
    } else {
        _top_level.root_typed_value_column = nullptr;
    }

    if (!shredded_already_read) {
        const std::vector<VariantPath>* requested_paths =
                _requested_shredded_paths.empty() ? nullptr : &_requested_shredded_paths;
        for (auto& node : _shredded_fields) {
            RETURN_IF_ERROR(_read_shredded_field_node(range, filter, &node, requested_paths));
        }
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
    if (_requested_shredded_paths.empty() && !_auto_paths_cached) {
        collect_all_top_binding_paths(_shredded_fields, &_cached_auto_paths);
        _auto_paths_cached = true;
    }
    const std::vector<VariantPath>& effective_paths =
            _requested_shredded_paths.empty() ? _cached_auto_paths : _requested_shredded_paths;

    std::vector<TopBinding> collected_bindings;
    collect_top_bindings(_shredded_fields, effective_paths, &collected_bindings);
    std::vector<TopBinding> materialized_bindings = select_materialized_bindings(collected_bindings, num_rows);
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
    NullColumn reconstructed_null_column(num_rows);
    auto& reconstructed_nulls = reconstructed_null_column.get_data();
    bool has_reconstructed_null = false;

    // batch_ctx is built first so has_typed_value_bitmap is available for both code paths.
    VariantReadRangeBatchContext batch_ctx(
            _shredded_fields, materialized_bindings, _top_level.root_typed_value_column.get(),
            _top_level.root_typed_value_type.get(), metadata_column, value_column, metadata_nulls, value_nulls);

    variant_column->set_shredded_columns(std::move(typed_paths), std::move(typed_types), std::move(typed_columns),
                                         BinaryColumn::create(), BinaryColumn::create());

    // Per-row materialization path keeps the top-level metadata/value as the base remain
    // payload. Shredded bindings are stored separately in typed_columns so full-row
    // reconstruction is deferred until a caller asks VariantColumn to materialize a row.
    const bool bulk_append_base_payload = batch_ctx.can_bulk_append_base_payload(dst->is_nullable());
    if (bulk_append_base_payload) {
        variant_column->metadata_column()->append(*metadata_column, 0, num_rows);
        variant_column->remain_value_column()->append(*value_column, 0, num_rows);
    }

    VariantReadRangeRowMaterializer materializer(batch_ctx, 0, variant_column);
    for (size_t i = 0; i < num_rows; ++i) {
        materializer.set_row(i);
        ASSIGN_OR_RETURN(bool prepared, materializer.prepare());
        if (!prepared) {
            if (bulk_append_base_payload) {
                append_null_to_typed_bindings(variant_column);
            } else {
                variant_column->append_shredded_null();
            }
            reconstructed_nulls[i] = 1;
            has_reconstructed_null = true;
            continue;
        }
        if (!bulk_append_base_payload) {
            materializer.append_top_level_row();
        }
        RETURN_IF_ERROR(materializer.append_bindings());
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

const ColumnReader* VariantColumnReader::filterable_typed_value_reader_for_path(const VariantPath& path) const {
    const ShreddedFieldNode* found_node = find_shredded_field_node_for_path(_shredded_fields, path);
    if (found_node == nullptr) return nullptr;
    if (found_node->kind != ShreddedFieldNode::Kind::SCALAR) return nullptr;
    if (found_node->typed_value_reader == nullptr) return nullptr;
    if (found_node->typed_value_read_type == nullptr) return nullptr;
    {
        LogicalType lt = found_node->typed_value_read_type->type;
        if (lt == TYPE_BINARY || lt == TYPE_VARBINARY) return nullptr;
    }
    return found_node->typed_value_reader.get();
}

ColumnReader* VariantColumnReader::scalar_typed_value_reader_for_path(const VariantPath& path) {
    const ShreddedFieldNode* found_node = find_shredded_field_node_for_path(_shredded_fields, path);
    if (found_node == nullptr) return nullptr;
    if (found_node->kind != ShreddedFieldNode::Kind::SCALAR) return nullptr;
    if (found_node->typed_value_reader == nullptr) return nullptr;
    return found_node->typed_value_reader.get();
}

const TypeDescriptor* VariantColumnReader::typed_value_read_type_for_path(const VariantPath& path) const {
    const ShreddedFieldNode* found_node = find_shredded_field_node_for_path(_shredded_fields, path);
    if (found_node == nullptr || found_node->kind != ShreddedFieldNode::Kind::SCALAR) return nullptr;
    return found_node->typed_value_read_type.get();
}

bool VariantColumnReader::fallback_values_all_null_in_row_group_for_path(const VariantPath& path,
                                                                         uint64_t rg_num_rows) const {
    const ShreddedFieldNode* found_node = find_shredded_field_node_for_path(_shredded_fields, path);
    if (found_node == nullptr || found_node->kind != ShreddedFieldNode::Kind::SCALAR) return false;
    if (found_node->value_reader == nullptr) return true;

    return _column_chunk_all_null_for_num_rows(found_node->value_reader.get(), rg_num_rows);
}

bool VariantVirtualZoneMapReader::_prepare_delegate_predicates(
        const std::vector<const ColumnPredicate*>& predicates, ObjectPool* pool, const uint64_t rg_num_rows,
        const ColumnReader** leaf_reader, std::vector<const ColumnPredicate*>* rewritten_predicates) const {
    DCHECK(pool != nullptr);
    DCHECK(leaf_reader != nullptr);
    DCHECK(rewritten_predicates != nullptr);
    *leaf_reader = nullptr;

    if (_source == nullptr) {
        VLOG_FILE << "skip variant virtual typed_value pushdown for path=" << _leaf_path.to_shredded_path().value_or("")
                  << " because source reader is unavailable";
        return false;
    }
    *leaf_reader = _source->filterable_typed_value_reader_for_path(_leaf_path);
    const TypeDescriptor* leaf_type = _source->typed_value_read_type_for_path(_leaf_path);
    if (*leaf_reader == nullptr || leaf_type == nullptr) {
        VLOG_FILE << "skip variant virtual typed_value pushdown for path=" << _leaf_path.to_shredded_path().value_or("")
                  << " because leaf reader/type is unavailable";
        return false;
    }
    // Variant shredding only permits data skipping on typed_value statistics when the paired
    // fallback `value` column is null for the entire row group. Otherwise min/max/bloom on
    // typed_value cover only the typed subset, while the engine may still match fallback rows
    // via implicit variant casts (for example Variant -> STRING), so skipping would be unsafe.
    if (!_source->fallback_values_all_null_in_row_group_for_path(_leaf_path, rg_num_rows)) {
        VLOG_FILE << "skip variant virtual typed_value pushdown for path=" << _leaf_path.to_shredded_path().value_or("")
                  << " because fallback value column may contain non-null rows in this row group";
        return false;
    }

    // TODO(variant): remove virtual-slot/leaf duality by materializing rewritten predicates
    // once during planning instead of per filter invocation.
    Status st = rewrite_delegate_predicates(predicates, *leaf_type, pool, rewritten_predicates);
    if (!st.ok()) {
        VLOG_FILE << "skip variant virtual typed_value pushdown for path=" << _leaf_path.to_shredded_path().value_or("")
                  << ", slot type " << _virtual_slot_type.debug_string() << " cannot delegate to leaf type "
                  << leaf_type->debug_string() << ": " << st.to_string();
        return false;
    }
    return true;
}

StatusOr<bool> VariantVirtualZoneMapReader::row_group_zone_map_filter(
        const std::vector<const ColumnPredicate*>& predicates, CompoundNodeType pred_relation,
        const uint64_t rg_first_row, const uint64_t rg_num_rows) const {
    ObjectPool pool;
    const ColumnReader* leaf = nullptr;
    std::vector<const ColumnPredicate*> rewritten_predicates;
    if (!_prepare_delegate_predicates(predicates, &pool, rg_num_rows, &leaf, &rewritten_predicates)) {
        return false;
    }
    return leaf->row_group_zone_map_filter(rewritten_predicates, pred_relation, rg_first_row, rg_num_rows);
}

StatusOr<bool> VariantVirtualZoneMapReader::page_index_zone_map_filter(
        const std::vector<const ColumnPredicate*>& predicates, SparseRange<uint64_t>* row_ranges,
        CompoundNodeType pred_relation, const uint64_t rg_first_row, const uint64_t rg_num_rows) {
    ObjectPool pool;
    const ColumnReader* leaf = nullptr;
    std::vector<const ColumnPredicate*> rewritten_predicates;
    if (!_prepare_delegate_predicates(predicates, &pool, rg_num_rows, &leaf, &rewritten_predicates)) {
        return false;
    }

    // page_index_zone_map_filter is non-const in the base class; cast is safe because the
    // underlying object is non-const (it's a reader owned by VariantColumnReader).
    return const_cast<ColumnReader*>(leaf)->page_index_zone_map_filter(rewritten_predicates, row_ranges, pred_relation,
                                                                       rg_first_row, rg_num_rows);
}

StatusOr<bool> VariantVirtualZoneMapReader::row_group_bloom_filter(
        const std::vector<const ColumnPredicate*>& predicates, CompoundNodeType pred_relation,
        const uint64_t rg_first_row, const uint64_t rg_num_rows) const {
    ObjectPool pool;
    const ColumnReader* leaf = nullptr;
    std::vector<const ColumnPredicate*> rewritten_predicates;
    if (!_prepare_delegate_predicates(predicates, &pool, rg_num_rows, &leaf, &rewritten_predicates)) {
        return false;
    }
    return leaf->row_group_bloom_filter(rewritten_predicates, pred_relation, rg_first_row, rg_num_rows);
}

} // namespace starrocks::parquet
