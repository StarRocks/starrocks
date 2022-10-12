// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/json_column.h"
#include "exprs/vectorized/cast_expr.h"
#include "gutil/casts.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "velocypack/Iterator.h"

namespace starrocks::vectorized {

static const char* kArrayDelimeter = ",";

ColumnPtr VectorizedCastArrayExpr::evaluate(ExprContext* context, vectorized::Chunk* ptr) {
    ColumnPtr column = _children[0]->evaluate(context, ptr);
    if (ColumnHelper::count_nulls(column) == column->size()) {
        return ColumnHelper::create_const_null_column(column->size());
    }
    ColumnPtr cast_column = column->clone_shared();
    ArrayColumn::Ptr array_col = nullptr;
    NullableColumn::Ptr nullable_col = nullptr;
    ColumnPtr src_col = cast_column;

    if (src_col->is_nullable()) {
        nullable_col = (ColumnHelper::as_column<NullableColumn>(src_col));
        src_col = nullable_col->data_column();
    }
    while (src_col->is_array()) {
        array_col = (ColumnHelper::as_column<ArrayColumn>(src_col));
        src_col = array_col->elements_column();
        if (src_col->is_nullable()) {
            nullable_col = (ColumnHelper::as_column<NullableColumn>(src_col));
            src_col = nullable_col->data_column();
        } else {
            nullable_col = nullptr;
        }
    }

    if (nullable_col != nullptr) {
        src_col = nullable_col;
    }
    ChunkPtr chunk = std::make_shared<Chunk>();
    auto column_ref = _cast_element_expr->get_child(0);
    SlotId slot_id = (reinterpret_cast<ColumnRef*>(column_ref))->slot_id();
    chunk->append_column(src_col, slot_id);
    ColumnPtr dest_col = _cast_element_expr->evaluate(nullptr, chunk.get());
    dest_col = ColumnHelper::unfold_const_column(column_ref->type(), chunk->num_rows(), dest_col);

    if (src_col->is_nullable() && !dest_col->is_nullable()) {
        // if the original column is nullable
        auto nullable_col = (ColumnHelper::as_column<NullableColumn>(src_col))->null_column();
        array_col->elements_column() = NullableColumn::create(dest_col, nullable_col);
    } else {
        array_col->elements_column() = dest_col;
    }
    return cast_column;
};

// Cast string to array<ANY>
ColumnPtr CastStringToArray::evaluate(ExprContext* context, vectorized::Chunk* input_chunk) {
    ColumnPtr column = _children[0]->evaluate(context, input_chunk);
    if (column->only_null()) {
        return ColumnHelper::create_const_null_column(column->size());
    }

    PrimitiveType element_type = _cast_elements_expr->type().type;
    ColumnViewer<TYPE_VARCHAR> src(column);
    UInt32Column::Ptr offsets = UInt32Column::create();
    NullColumn::Ptr null_column = NullColumn::create();

    // 1. Split string with ',' delimiter
    uint32_t offset = 0;
    ColumnBuilder<TYPE_VARCHAR> slice_builder(src.size());
    for (size_t i = 0; i < src.size(); i++) {
        offsets->append(offset);
        if (src.is_null(i)) {
            null_column->append(1);
            continue;
        }
        null_column->append(0);
        Slice str = src.value(i);
        if (str.starts_with("[")) {
            str.data++;
            str.size--;
        }
        if (str.ends_with("]")) {
            str.size--;
        }
        if (!str.empty()) {
            StringPiece str_piece(str.data, str.size);
            std::vector<StringPiece> pieces;
            SplitStringPieceToVector(str_piece, kArrayDelimeter, &pieces, false);

            // Unquote slice for string type
            if (element_type == TYPE_VARCHAR || element_type == TYPE_CHAR) {
                for (auto& piece : pieces) {
                    slice_builder.append(_unquote(Slice(piece.data(), piece.size())));
                }
            } else {
                for (auto& piece : pieces) {
                    slice_builder.append(Slice(piece.data(), piece.size()));
                }
            }
            offset += pieces.size();
        }
    }
    offsets->append(offset);

    // 2. Cast string to specified type
    ColumnPtr elements = slice_builder.build_nullable_column();
    if (element_type != TYPE_VARCHAR && element_type != TYPE_CHAR) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        SlotId slot_id = down_cast<ColumnRef*>(_cast_elements_expr->get_child(0))->slot_id();
        chunk->append_column(elements, slot_id);
        elements = ColumnHelper::cast_to_nullable_column(_cast_elements_expr->evaluate(context, chunk.get()));
    }

    // 3. Assemble elements into array column
    ColumnPtr res = ArrayColumn::create(elements, offsets);
    if (column->is_nullable()) {
        res = NullableColumn::create(res, null_column);
    }

    return res;
}

Slice CastStringToArray::_unquote(Slice slice) {
    while (slice.starts_with(" ")) {
        slice.remove_prefix(1);
    }
    while (slice.ends_with(" ")) {
        slice.remove_suffix(1);
    }
    if ((slice.starts_with("\"") && slice.ends_with("\"")) || (slice.starts_with("'") && slice.ends_with("'"))) {
        slice.remove_prefix(1);
        slice.remove_suffix(1);
    }
    return slice;
}

ColumnPtr CastJsonToArray::evaluate(ExprContext* context, vectorized::Chunk* input_chunk) {
    ColumnPtr column = _children[0]->evaluate(context, input_chunk);
    if (column->only_null()) {
        return ColumnHelper::create_const_null_column(column->size());
    }

    PrimitiveType element_type = _cast_elements_expr->type().type;
    ColumnViewer<TYPE_JSON> src(column);
    UInt32Column::Ptr offsets = UInt32Column::create();
    NullColumn::Ptr null_column = NullColumn::create();

    // 1. Cast JsonArray to ARRAY<JSON>
    uint32_t offset = 0;
    ColumnBuilder<TYPE_JSON> json_column_builder(src.size());
    for (size_t i = 0; i < src.size(); i++) {
        offsets->append(offset);
        if (src.is_null(i)) {
            null_column->append(1);
            continue;
        }
        null_column->append(0);
        const JsonValue* json_value = src.value(i);
        if (json_value && json_value->get_type() == JsonType::JSON_ARRAY) {
            vpack::Slice json_slice = json_value->to_vslice();
            DCHECK(json_slice.isArray());
            for (const auto& element : vpack::ArrayIterator(json_slice)) {
                JsonValue element_value(element);
                json_column_builder.append(std::move(element_value));
            }
            offset += json_slice.length();
        } else {
            null_column->append(1);
        }
    }
    offsets->append(offset);

    // 2. Cast json to specified type
    ColumnPtr elements = json_column_builder.build_nullable_column();
    if (element_type != TYPE_JSON) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        SlotId slot_id = down_cast<ColumnRef*>(_cast_elements_expr->get_child(0))->slot_id();
        chunk->append_column(elements, slot_id);
        elements = ColumnHelper::cast_to_nullable_column(_cast_elements_expr->evaluate(context, chunk.get()));
    }

    // 3. Assemble elements into array column
    ColumnPtr res = ArrayColumn::create(elements, offsets);
    if (column->is_nullable()) {
        res = NullableColumn::create(res, null_column);
    }

    return res;
}

} // namespace starrocks::vectorized