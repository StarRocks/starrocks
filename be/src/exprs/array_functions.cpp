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

#include "exprs/array_functions.h"

#include "column/array_column.h"
#include "column/column_hash.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "common/statusor.h"
#include "simd/simd.h"
#include "util/raw_container.h"

namespace starrocks {

StatusOr<ColumnPtr> ArrayFunctions::array_length([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    Column* arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]).get();
    const size_t num_rows = arg0->size();

    auto* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(arg0));

    auto col_result = Int32Column::create();
    raw::make_room(&col_result->get_data(), num_rows);
    DCHECK_EQ(num_rows, col_result->size());

    const uint32_t* offsets = col_array->offsets().get_data().data();

    int32_t* p = col_result->get_data().data();
    for (size_t i = 0; i < num_rows; i++) {
        p[i] = offsets[i + 1] - offsets[i];
    }

    if (arg0->has_null()) {
        // Copy null flags.
        return NullableColumn::create(std::move(col_result), down_cast<NullableColumn*>(arg0)->null_column());
    } else {
        return col_result;
    }
}

StatusOr<ColumnPtr> ArrayFunctions::array_ndims([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return nullptr;
}

template <bool OnlyNullData, bool ConstData>
static StatusOr<ColumnPtr> do_array_append(const Column& elements, const UInt32Column& offsets, const Column& data) {
    size_t num_array = offsets.size() - 1;
    uint32_t curr_offset = 0;

    [[maybe_unused]] const Column* const_data = &data;
    if constexpr (ConstData) {
        const_data = down_cast<const ConstColumn*>(&data)->data_column().get();
    }

    auto result_array = ArrayColumn::create(elements.clone_empty(), UInt32Column::create());
    UInt32Column::Container& result_offsets = result_array->offsets_column()->get_data();
    ColumnPtr& result_elements = result_array->elements_column();
    result_elements->reserve(elements.size() + data.size());
    result_offsets.reserve(num_array + 1);

    uint32_t result_offset = 0;
    for (size_t i = 0; i < num_array; i++) {
        uint32_t next_offset = offsets.get_data()[i + 1];
        uint32_t array_size = next_offset - curr_offset;
        result_elements->append(elements, curr_offset, array_size);
        if constexpr (OnlyNullData) {
            (void)result_elements->append_nulls(1);
        } else if constexpr (ConstData) {
            result_elements->append(*const_data, 0, 1);
        } else {
            result_elements->append(data, i, 1);
        }
        result_offset += array_size + 1;
        result_offsets.push_back(result_offset);
        curr_offset = next_offset;
    }
    return result_array;
}

// FIXME: A proof-of-concept implementation with poor performance.
StatusOr<ColumnPtr> ArrayFunctions::array_append([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    if (columns[0]->only_null()) {
        return columns[0];
    }

    const Column* arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]).get();
    const Column* arg1 = columns[1].get();

    const Column* array = arg0;
    const NullableColumn* nullable_array = nullptr;

    if (arg0->is_nullable()) {
        nullable_array = down_cast<const NullableColumn*>(arg0);
        array = nullable_array->data_column().get();
    }

    const Column* element = &down_cast<const ArrayColumn*>(array)->elements();
    const UInt32Column* offsets = &down_cast<const ArrayColumn*>(array)->offsets();
    bool const_data = arg1->is_constant();

    StatusOr<ColumnPtr> result;
    if (arg1->only_null()) {
        result = do_array_append<true, true>(*element, *offsets, *arg1);
    } else if (const_data) {
        result = do_array_append<false, true>(*element, *offsets, *arg1);
    } else {
        result = do_array_append<false, false>(*element, *offsets, *arg1);
    }
    RETURN_IF_ERROR(result);

    if (nullable_array != nullptr) {
        return NullableColumn::create(std::move(result.value()), nullable_array->null_column());
    }
    return result;
}

class ArrayRemoveImpl {
public:
    static StatusOr<ColumnPtr> evaluate(const ColumnPtr& array, const ColumnPtr& element) {
        return _array_remove_generic(array, element);
    }

private:
    template <bool NullableElement, bool NullableTarget, bool ConstTarget, typename ElementColumn,
              typename TargetColumn>
    static StatusOr<ColumnPtr> _process(const ElementColumn& elements, const UInt32Column& offsets,
                                        const TargetColumn& targets, const NullColumn::Container* null_map_elements,
                                        const NullColumn::Container* null_map_targets) {
        const size_t num_array = offsets.size() - 1;

        auto result_array = ArrayColumn::create(NullableColumn::create(elements.clone_empty(), NullColumn::create()),
                                                UInt32Column::create());
        UInt32Column::Container& result_offsets = result_array->offsets_column()->get_data();
        ColumnPtr& result_elements = result_array->elements_column();

        result_offsets.reserve(num_array);

        using ValueType = std::conditional_t<std::is_same_v<ArrayColumn, ElementColumn> ||
                                                     std::is_same_v<MapColumn, ElementColumn> ||
                                                     std::is_same_v<StructColumn, ElementColumn>,
                                             uint8_t, typename ElementColumn::ValueType>;

        auto offsets_ptr = offsets.get_data().data();
        [[maybe_unused]] auto is_null = [](const NullColumn::Container* null_map, size_t idx) -> bool {
            return (*null_map)[idx] != 0;
        };

        uint32_t result_offset = 0;
        for (size_t i = 0; i < num_array; i++) {
            size_t offset = offsets_ptr[i];
            size_t array_size = offsets_ptr[i + 1] - offsets_ptr[i];

            // element: [], target: 1
            if (array_size == 0) {
                result_elements->append(elements, offset, array_size);
                result_offsets.push_back(result_offset);
                continue;
            }

            uint32_t total_found = 0;

            for (size_t j = 0; j < array_size; j++) {
                // element: [NULL, 1], target: 1
                if constexpr (NullableElement && !NullableTarget) {
                    if (is_null(null_map_elements, offset + j)) {
                        (void)result_elements->append_nulls(1);
                        continue;
                    }
                }

                // element: [1,2], target: NULL
                if constexpr (!NullableElement && NullableTarget) {
                    if (is_null(null_map_targets, i)) {
                        result_elements->append(elements, offset + j, 1);
                        continue;
                    }
                }

                // element: [NULL, 1], target: 1
                if constexpr (NullableElement && NullableTarget) {
                    bool null_element = is_null(null_map_elements, offset + j);
                    bool null_target = is_null(null_map_targets, i);
                    if (null_element && !null_target) {
                        (void)result_elements->append_nulls(1);
                    } else if (!null_element && null_target) {
                        result_elements->append(elements, offset + j, 1);
                    } else {
                        total_found++;
                    }

                    continue;
                }

                uint8_t found = 0;
                if constexpr (std::is_same_v<ArrayColumn, ElementColumn> || std::is_same_v<MapColumn, ElementColumn> ||
                              std::is_same_v<StructColumn, ElementColumn> ||
                              std::is_same_v<JsonColumn, ElementColumn>) {
                    found = elements.equals(offset + j, targets, i);
                } else if constexpr (ConstTarget) {
                    [[maybe_unused]] auto elements_ptr = (const ValueType*)(elements.raw_data());
                    auto targets_ptr = (const ValueType*)(targets.raw_data());
                    auto& first_target = *targets_ptr;
                    found = (elements_ptr[offset + j] == first_target);
                } else {
                    [[maybe_unused]] auto elements_ptr = (const ValueType*)(elements.raw_data());
                    auto targets_ptr = (const ValueType*)(targets.raw_data());
                    found = (elements_ptr[offset + j] == targets_ptr[i]);
                }

                if (found == 1) {
                    total_found++;
                } else {
                    result_elements->append(elements, offset + j, 1);
                }
            }

            result_offset += array_size - total_found;
            result_offsets.push_back(result_offset);
        }

        return result_array;
    }

    template <bool NullableElement, bool NullableTarget, bool ConstTarget>
    static StatusOr<ColumnPtr> _array_remove(const Column& array_elements, const UInt32Column& array_offsets,
                                             const Column& argument) {
        const Column* elements_ptr = &array_elements;
        const Column* targets_ptr = &argument;

        const NullColumn::Container* null_map_elements = nullptr;
        const NullColumn::Container* null_map_targets = nullptr;

        if constexpr (NullableElement) {
            const auto& nullable = down_cast<const NullableColumn&>(array_elements);
            elements_ptr = nullable.data_column().get();
            null_map_elements = &(nullable.null_column()->get_data());
        }

        if constexpr (NullableTarget) {
            const auto& nullable = down_cast<const NullableColumn&>(argument);
            targets_ptr = nullable.data_column().get();
            null_map_targets = &(nullable.null_column()->get_data());
        }

#define HANDLE_ELEMENT_TYPE(ElementType)                                                                          \
    do {                                                                                                          \
        if (typeid(*elements_ptr) == typeid(ElementType)) {                                                       \
            return _process<NullableElement, NullableTarget, ConstTarget>(                                        \
                    *down_cast<const ElementType*>(elements_ptr), array_offsets, *targets_ptr, null_map_elements, \
                    null_map_targets);                                                                            \
        }                                                                                                         \
    } while (0)

        HANDLE_ELEMENT_TYPE(BooleanColumn);
        HANDLE_ELEMENT_TYPE(Int8Column);
        HANDLE_ELEMENT_TYPE(Int16Column);
        HANDLE_ELEMENT_TYPE(Int32Column);
        HANDLE_ELEMENT_TYPE(Int64Column);
        HANDLE_ELEMENT_TYPE(Int128Column);
        HANDLE_ELEMENT_TYPE(FloatColumn);
        HANDLE_ELEMENT_TYPE(DoubleColumn);
        HANDLE_ELEMENT_TYPE(DecimalColumn);
        HANDLE_ELEMENT_TYPE(Decimal32Column);
        HANDLE_ELEMENT_TYPE(Decimal64Column);
        HANDLE_ELEMENT_TYPE(Decimal128Column);
        HANDLE_ELEMENT_TYPE(BinaryColumn);
        HANDLE_ELEMENT_TYPE(DateColumn);
        HANDLE_ELEMENT_TYPE(TimestampColumn);
        HANDLE_ELEMENT_TYPE(ArrayColumn);
        HANDLE_ELEMENT_TYPE(JsonColumn);
        HANDLE_ELEMENT_TYPE(MapColumn);
        HANDLE_ELEMENT_TYPE(StructColumn);

        return Status::NotSupported("unsupported operation for type: " + array_elements.get_name());
    }

    // array is non-nullable.
    static StatusOr<ColumnPtr> _array_remove_non_nullable(const ArrayColumn& array, const Column& arg) {
        bool nullable_element = false;
        bool nullable_target = false;
        bool const_target = false;
        ColumnPtr targets_holder;

        const UInt32Column& offsets = array.offsets();
        const Column* elements = &array.elements();
        const Column* targets = &arg;
        if (auto nullable = dynamic_cast<const NullableColumn*>(elements); nullable != nullptr) {
            // If this nullable column does NOT contains any NULL, process it as non-nullable column.
            nullable_element = nullable->has_null();
            elements = nullable->has_null() ? elements : nullable->data_column().get();
        }
        if (auto nullable = dynamic_cast<const NullableColumn*>(targets); nullable != nullptr) {
            nullable_target = nullable->has_null();
            targets = nullable->has_null() ? targets : nullable->data_column().get();
        }

        // element: [1,2]; target: NULL
        if (targets->only_null() && !nullable_element) {
            return ArrayColumn::create(array);
        }

        // Expand Only-Null column.
        if (targets->only_null()) {
            auto data = down_cast<const NullableColumn*>(elements)->data_column()->clone_empty();
            targets_holder = NullableColumn::create(std::move(data), NullColumn::create());
            (void)targets_holder->append_nulls(array.size());
            targets = targets_holder.get();
            nullable_target = true;
        }

        if (targets->is_constant()) {
            const_target = true;
        }

        DCHECK(!(const_target && nullable_target));

        if (nullable_element && nullable_target) {
            return _array_remove<true, true, false>(*elements, offsets, *targets);
        } else if (nullable_element) {
            return const_target ? _array_remove<true, false, true>(*elements, offsets, *targets)
                                : _array_remove<true, false, false>(*elements, offsets, *targets);
        } else if (nullable_target) {
            return _array_remove<false, true, false>(*elements, offsets, *targets);
        } else {
            return const_target ? _array_remove<false, false, true>(*elements, offsets, *targets)
                                : _array_remove<false, false, false>(*elements, offsets, *targets);
        }
    }

    static StatusOr<ColumnPtr> _array_remove_generic(const ColumnPtr& array, const ColumnPtr& target) {
        if (array->is_nullable()) {
            auto nullable = down_cast<const NullableColumn*>(array.get());
            auto array_col = down_cast<const ArrayColumn*>(nullable->data_column().get());
            ASSIGN_OR_RETURN(auto result, _array_remove_non_nullable(*array_col, *target))
            DCHECK_EQ(nullable->size(), result->size());
            return NullableColumn::create(std::move(result), nullable->null_column());
        }

        return _array_remove_non_nullable(down_cast<ArrayColumn&>(*array), *target);
    }
};

template <LogicalType TYPE>
struct ArrayCumSumImpl {
public:
    static StatusOr<ColumnPtr> evaluate(const ColumnPtr& col) {
        if (col->is_constant()) {
            auto* input = down_cast<ConstColumn*>(col.get());
            auto arr_col_h = input->data_column()->clone();
            auto* arr_col = down_cast<ArrayColumn*>(arr_col_h.get());
            call_cum_sum(arr_col, nullptr);
            return ConstColumn::create(std::move(arr_col_h), input->size());
        } else if (col->is_nullable()) {
            auto res = col->clone();
            auto* input = down_cast<NullableColumn*>(res.get());
            NullColumn* null_column = input->mutable_null_column();
            auto* arr_col = down_cast<ArrayColumn*>(input->data_column().get());
            call_cum_sum(arr_col, null_column);
            return res;
        } else {
            auto res = col->clone();
            auto* arr_col = down_cast<ArrayColumn*>(res.get());
            call_cum_sum(arr_col, nullptr);
            return res;
        }
    }

private:
    static void call_cum_sum(ArrayColumn* arr_col, NullColumn* null_column) {
        bool is_nullable = null_column != nullptr;
        bool element_nullable = arr_col->elements_column()->is_nullable();
        if (is_nullable && element_nullable) {
            cumu_sum<true, true>(arr_col, null_column);
        } else if (is_nullable && !element_nullable) {
            cumu_sum<true, false>(arr_col, null_column);
        } else if (!is_nullable && element_nullable) {
            cumu_sum<false, true>(arr_col, null_column);
        } else {
            cumu_sum<false, false>(arr_col, null_column);
        }
    }

    template <bool nullable, bool element_nullable>
    static void cumu_sum(ArrayColumn* arr_col, NullColumn* null_column) {
        auto* element = arr_col->elements_column().get();
        if (element->size() == 0) {
            return;
        }
        auto* element_data =
                element->is_nullable()
                        ? ColumnHelper::get_cpp_data<TYPE>(down_cast<NullableColumn*>(element)->data_column())
                        : ColumnHelper::get_cpp_data<TYPE>(arr_col->elements_column());
        auto* element_null_data =
                element->is_nullable() ? down_cast<NullableColumn*>(element)->null_column_data().data() : nullptr;
        auto& offsets = arr_col->offsets().get_data();
        size_t num_rows = offsets.size() - 1;
        NullColumn::Container::pointer null_data = null_column ? null_column->get_data().data() : nullptr;

        if constexpr (element_nullable) {
            DCHECK(element_null_data != nullptr);
        } else {
            DCHECK(element_null_data == nullptr);
        }

        for (int i = 0; i < num_rows; ++i) {
            if constexpr (nullable) {
                DCHECK(null_data != nullptr);
                if (null_data[i]) {
                    continue;
                }
            }
            RunTimeCppType<TYPE> cum_sum{}; // TODO: to solve overflow
            for (int j = offsets[i]; j < offsets[i + 1]; ++j) {
                if constexpr (element_nullable) {
                    if (element_null_data[j]) {
                        // skip null
                    } else {
                        cum_sum += element_data[j];
                    }
                } else {
                    cum_sum += element_data[j];
                }
                element_data[j] = cum_sum;
            }
        }
    }
};

#define DEFINE_ARRAY_CUMSUM_FN(NAME, TYPE)                                                              \
    StatusOr<ColumnPtr> ArrayFunctions::array_cum_sum_##NAME([[maybe_unused]] FunctionContext* context, \
                                                             const Columns& columns) {                  \
        DCHECK_EQ(columns.size(), 1);                                                                   \
        RETURN_IF_COLUMNS_ONLY_NULL(columns);                                                           \
        const ColumnPtr& arg0 = columns[0];                                                             \
                                                                                                        \
        return ArrayCumSumImpl<TYPE>::evaluate(arg0);                                                   \
    }

DEFINE_ARRAY_CUMSUM_FN(bigint, TYPE_BIGINT)
DEFINE_ARRAY_CUMSUM_FN(double, TYPE_DOUBLE)

#undef DEFINE_ARRAY_CUMSUM_FN

StatusOr<ColumnPtr> ArrayFunctions::array_remove([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL({columns[0]});
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = columns[1];                                                                      // element

    return ArrayRemoveImpl::evaluate(arg0, arg1);
}

// If PositionEnabled=true and ReturnType=Int32, it is function array_position and it will return index of elemt if the array contain it or 0 if not contain.
// If PositionEnabled=false and ReturnType=UInt8, it is function array_contains and it will return 1 if contain or 0 if not contain.
template <bool PositionEnabled, typename ReturnType>
class ArrayContainsImpl {
public:
    static StatusOr<ColumnPtr> evaluate(const Column& array, const Column& element) {
        return _array_contains_generic(array, element);
    }

private:
    template <bool NullableElement, bool NullableTarget, bool ConstTarget, typename ElementColumn,
              typename TargetColumn>
    static StatusOr<ColumnPtr> _process(const ElementColumn& elements, const UInt32Column& offsets,
                                        const TargetColumn& targets, const NullColumn::Container* null_map_elements,
                                        const NullColumn::Container* null_map_targets) {
        const size_t num_array = offsets.size() - 1;
        auto result = ReturnType::create();
        result->resize(num_array);

        auto* result_ptr = result->get_data().data();

        using ValueType = std::conditional_t<std::is_same_v<ArrayColumn, ElementColumn> ||
                                                     std::is_same_v<MapColumn, ElementColumn> ||
                                                     std::is_same_v<StructColumn, ElementColumn>,
                                             uint8_t, typename ElementColumn::ValueType>;

        auto offsets_ptr = offsets.get_data().data();

        [[maybe_unused]] auto is_null = [](const NullColumn::Container* null_map, size_t idx) -> bool {
            return (*null_map)[idx] != 0;
        };

        for (size_t i = 0; i < num_array; i++) {
            size_t offset = offsets_ptr[i];
            size_t array_size = offsets_ptr[i + 1] - offsets_ptr[i];
            uint8_t found = 0;
            size_t position = 0;
            for (size_t j = 0; j < array_size; j++) {
                if constexpr (NullableElement && !NullableTarget) {
                    if (is_null(null_map_elements, offset + j)) {
                        continue;
                    }
                }
                if constexpr (!NullableElement && NullableTarget) {
                    if (is_null(null_map_targets, i)) {
                        continue;
                    }
                }
                if constexpr (NullableElement && NullableTarget) {
                    bool null_element = is_null(null_map_elements, offset + j);
                    bool null_target = is_null(null_map_targets, i);
                    if (null_element != null_target) {
                        continue;
                    }
                    if (null_element) {
                        position = j + 1;
                        found = 1;
                        break;
                    }
                }
                if constexpr (std::is_same_v<ArrayColumn, ElementColumn> || std::is_same_v<MapColumn, ElementColumn> ||
                              std::is_same_v<StructColumn, ElementColumn> ||
                              std::is_same_v<JsonColumn, ElementColumn>) {
                    found = elements.equals(offset + j, targets, i);
                } else if constexpr (ConstTarget) {
                    [[maybe_unused]] auto elements_ptr = (const ValueType*)(elements.raw_data());
                    auto targets_ptr = (const ValueType*)(targets.raw_data());
                    auto& first_target = *targets_ptr;
                    found = (elements_ptr[offset + j] == first_target);
                } else {
                    [[maybe_unused]] auto elements_ptr = (const ValueType*)(elements.raw_data());
                    auto targets_ptr = (const ValueType*)(targets.raw_data());
                    found = (elements_ptr[offset + j] == targets_ptr[i]);
                }
                if (found) {
                    position = j + 1;
                    break;
                }
            }
            result_ptr[i] = PositionEnabled ? position : found;
        }
        return result;
    }

    template <bool NullableElement, bool NullableTarget, bool ConstTarget>
    static StatusOr<ColumnPtr> _array_contains(const Column& array_elements, const UInt32Column& array_offsets,
                                               const Column& argument) {
        const Column* elements_ptr = &array_elements;
        const Column* targets_ptr = &argument;

        const NullColumn::Container* null_map_elements = nullptr;
        const NullColumn::Container* null_map_targets = nullptr;

        if constexpr (NullableElement) {
            const auto& nullable = down_cast<const NullableColumn&>(array_elements);
            elements_ptr = nullable.data_column().get();
            null_map_elements = &(nullable.null_column()->get_data());
        }

        if constexpr (NullableTarget) {
            const auto& nullable = down_cast<const NullableColumn&>(argument);
            targets_ptr = nullable.data_column().get();
            null_map_targets = &(nullable.null_column()->get_data());
        }

        // Using typeid instead of dynamic_cast, typeid will be much much faster than dynamic_cast
#define HANDLE_ELEMENT_TYPE(ElementType)                                                                          \
    do {                                                                                                          \
        if (typeid(*elements_ptr) == typeid(ElementType)) {                                                       \
            return _process<NullableElement, NullableTarget, ConstTarget>(                                        \
                    *down_cast<const ElementType*>(elements_ptr), array_offsets, *targets_ptr, null_map_elements, \
                    null_map_targets);                                                                            \
        }                                                                                                         \
    } while (0)

        HANDLE_ELEMENT_TYPE(BooleanColumn);
        HANDLE_ELEMENT_TYPE(Int8Column);
        HANDLE_ELEMENT_TYPE(Int16Column);
        HANDLE_ELEMENT_TYPE(Int32Column);
        HANDLE_ELEMENT_TYPE(Int64Column);
        HANDLE_ELEMENT_TYPE(Int128Column);
        HANDLE_ELEMENT_TYPE(FloatColumn);
        HANDLE_ELEMENT_TYPE(DoubleColumn);
        HANDLE_ELEMENT_TYPE(DecimalColumn);
        HANDLE_ELEMENT_TYPE(Decimal32Column);
        HANDLE_ELEMENT_TYPE(Decimal64Column);
        HANDLE_ELEMENT_TYPE(Decimal128Column);
        HANDLE_ELEMENT_TYPE(BinaryColumn);
        HANDLE_ELEMENT_TYPE(DateColumn);
        HANDLE_ELEMENT_TYPE(TimestampColumn);
        HANDLE_ELEMENT_TYPE(ArrayColumn);
        HANDLE_ELEMENT_TYPE(JsonColumn);
        HANDLE_ELEMENT_TYPE(MapColumn);
        HANDLE_ELEMENT_TYPE(StructColumn);

        return Status::NotSupported("unsupported operation for type: " + array_elements.get_name());
    }

    // array is non-nullable.
    static StatusOr<ColumnPtr> _array_contains_non_nullable(const ArrayColumn& array, const Column& arg) {
        bool nullable_element = false;
        bool nullable_target = false;
        bool const_target = false;
        ColumnPtr targets_holder;

        const UInt32Column& offsets = array.offsets();
        const Column* elements = &array.elements();
        const Column* targets = &arg;
        if (auto nullable = dynamic_cast<const NullableColumn*>(elements); nullable != nullptr) {
            // If this nullable column does NOT contains any NULL, process it as non-nullable column.
            nullable_element = nullable->has_null();
            elements = nullable->has_null() ? elements : nullable->data_column().get();
        }
        if (auto nullable = dynamic_cast<const NullableColumn*>(targets); nullable != nullptr) {
            nullable_target = nullable->has_null();
            targets = nullable->has_null() ? targets : nullable->data_column().get();
        }
        if (targets->only_null() && !nullable_element) {
            auto result = ReturnType::create();
            result->resize(array.size());
            return result;
        }
        // Expand Only-Null column.
        if (targets->only_null()) {
            auto data = down_cast<const NullableColumn*>(elements)->data_column()->clone_empty();
            targets_holder = NullableColumn::create(std::move(data), NullColumn::create());
            (void)targets_holder->append_nulls(array.size());
            targets = targets_holder.get();
            nullable_target = true;
        }
        if (targets->is_constant()) {
            const_target = true;
        }

        CHECK(!(const_target && nullable_target));

        if (nullable_element && nullable_target) {
            return _array_contains<true, true, false>(*elements, offsets, *targets);
        } else if (nullable_element) {
            return const_target ? _array_contains<true, false, true>(*elements, offsets, *targets)
                                : _array_contains<true, false, false>(*elements, offsets, *targets);
        } else if (nullable_target) {
            return _array_contains<false, true, false>(*elements, offsets, *targets);
        } else {
            return const_target ? _array_contains<false, false, true>(*elements, offsets, *targets)
                                : _array_contains<false, false, false>(*elements, offsets, *targets);
        }
    }

    static StatusOr<ColumnPtr> _array_contains_generic(const Column& array, const Column& target) {
        if (array.is_nullable()) {
            auto nullable = down_cast<const NullableColumn*>(&array);
            auto array_col = down_cast<const ArrayColumn*>(nullable->data_column().get());
            ASSIGN_OR_RETURN(auto result, _array_contains_non_nullable(*array_col, target))
            DCHECK_EQ(nullable->size(), result->size());
            if (!nullable->has_null()) {
                return result;
            }
            return NullableColumn::create(std::move(result), nullable->null_column());
        }
        return _array_contains_non_nullable(down_cast<const ArrayColumn&>(array), target);
    }
};

template <bool Any>
class ArrayHasImpl {
public:
    static StatusOr<ColumnPtr> evaluate(const Column& array, const Column& element) {
        return _array_has_generic(array, element);
    }

private:
    template <bool NullableElement, bool NullableTarget, typename ElementColumn>
    static uint8 __process(const ElementColumn& elements, uint32 element_start, uint32 element_end,
                           const ElementColumn& targets, uint32 target_start, uint32 target_end,
                           const NullColumn::Container* null_map_elements,
                           const NullColumn::Container* null_map_targets) {
        using ValueType = std::conditional_t<std::is_same_v<ArrayColumn, ElementColumn> ||
                                                     std::is_same_v<MapColumn, ElementColumn> ||
                                                     std::is_same_v<StructColumn, ElementColumn>,
                                             uint8_t, typename ElementColumn::ValueType>;

        [[maybe_unused]] auto is_null = [](const NullColumn::Container* null_map, size_t idx) -> bool {
            return (*null_map)[idx] != 0;
        };
        for (size_t i = target_start; i < target_end; i++) {
            bool null_target = false;
            if constexpr (NullableTarget) {
                null_target = is_null(null_map_targets, i);
            }

            if constexpr (!NullableElement) {
                // [x, x, x] - [null*, x, x]
                if (null_target) {
                    if constexpr (!Any) {
                        return false;
                    }
                    continue;
                }
            }
            bool found = false;
            for (size_t j = element_start; j < element_end; j++) {
                bool null_element = false;
                if constexpr (NullableElement) {
                    null_element = is_null(null_map_elements, j);
                }

                // [null*, x, x] - [null*, x, x]
                if (null_element && null_target) {
                    if constexpr (Any) {
                        return true;
                    }
                    found = true;
                    break;
                }
                // [null*, x, x] - [null, x*, x]
                if (null_element != null_target) {
                    continue;
                }
                //[null, x*, x] - [null, x*, x]
                if constexpr (std::is_same_v<ArrayColumn, ElementColumn> || std::is_same_v<MapColumn, ElementColumn> ||
                              std::is_same_v<StructColumn, ElementColumn> ||
                              std::is_same_v<JsonColumn, ElementColumn>) {
                    found = (elements.equals(j, targets, i) == 1);
                } else {
                    auto elements_ptr = (const ValueType*)(elements.raw_data());
                    auto targets_ptr = (const ValueType*)(targets.raw_data());
                    found = (elements_ptr[j] == targets_ptr[i]);
                }
                if (found) {
                    if constexpr (Any) {
                        return true;
                    }
                    break;
                }
            }
            if constexpr (!Any) {
                if (!found) {
                    return false;
                }
            }
        }
        if constexpr (Any) {
            return false;
        } else {
            return true;
        }
    }
    template <bool NullableElement, bool NullableTarget, bool ConstTarget, typename ElementColumn>
    static StatusOr<ColumnPtr> _process(const ElementColumn& elements, const UInt32Column& element_offsets,
                                        const ElementColumn& targets, const UInt32Column& target_offsets,
                                        const NullColumn::Container* null_map_elements,
                                        const NullColumn::Container* null_map_targets) {
        const size_t num_array = element_offsets.size() - 1;
        const size_t num_target = target_offsets.size() - 1;
        auto result = UInt8Column::create();
        result->resize(num_array);

        auto* result_ptr = result->get_data().data();

        auto element_offsets_ptr = element_offsets.get_data().data();
        auto target_offsets_ptr = target_offsets.get_data().data();

        for (size_t i = 0; i < num_array; i++) {
            uint8_t found = 0;
            if constexpr (ConstTarget) {
                DCHECK_EQ(num_target, 1);
                found = __process<NullableElement, NullableTarget, ElementColumn>(
                        elements, element_offsets_ptr[i], element_offsets_ptr[i + 1], targets, target_offsets_ptr[0],
                        target_offsets_ptr[1], null_map_elements, null_map_targets);
            } else {
                DCHECK_EQ(num_array, num_target);
                found = __process<NullableElement, NullableTarget, ElementColumn>(
                        elements, element_offsets_ptr[i], element_offsets_ptr[i + 1], targets, target_offsets_ptr[i],
                        target_offsets_ptr[i + 1], null_map_elements, null_map_targets);
            }
            result_ptr[i] = found;
        }
        return result;
    }

    template <bool NullableElement, bool NullableTarget, bool ConstTarget>
    static StatusOr<ColumnPtr> _array_has(const Column& array_elements, const UInt32Column& array_offsets,
                                          const Column& array_targets, const UInt32Column& target_offsets) {
        const Column* elements_ptr = &array_elements;
        const Column* targets_ptr = &array_targets;

        const NullColumn::Container* null_map_elements = nullptr;
        const NullColumn::Container* null_map_targets = nullptr;

        if constexpr (NullableElement) {
            const auto& nullable = down_cast<const NullableColumn&>(array_elements);
            elements_ptr = nullable.data_column().get();
            null_map_elements = &(nullable.null_column()->get_data());
        }

        if constexpr (NullableTarget) {
            const auto& nullable = down_cast<const NullableColumn&>(array_targets);
            targets_ptr = nullable.data_column().get();
            null_map_targets = &(nullable.null_column()->get_data());
        }

        // Using typeid instead of dynamic_cast, typeid will be much much faster than dynamic_cast
#define HANDLE_HAS_TYPE(ElementType)                                                                                   \
    do {                                                                                                               \
        if (typeid(*elements_ptr) == typeid(ElementType)) {                                                            \
            return _process<NullableElement, NullableTarget, ConstTarget>(                                             \
                    *down_cast<const ElementType*>(elements_ptr), array_offsets,                                       \
                    *down_cast<const ElementType*>(targets_ptr), target_offsets, null_map_elements, null_map_targets); \
        }                                                                                                              \
    } while (0)

        HANDLE_HAS_TYPE(BooleanColumn);
        HANDLE_HAS_TYPE(Int8Column);
        HANDLE_HAS_TYPE(Int16Column);
        HANDLE_HAS_TYPE(Int32Column);
        HANDLE_HAS_TYPE(Int64Column);
        HANDLE_HAS_TYPE(Int128Column);
        HANDLE_HAS_TYPE(FloatColumn);
        HANDLE_HAS_TYPE(DoubleColumn);
        HANDLE_HAS_TYPE(DecimalColumn);
        HANDLE_HAS_TYPE(Decimal32Column);
        HANDLE_HAS_TYPE(Decimal64Column);
        HANDLE_HAS_TYPE(Decimal128Column);
        HANDLE_HAS_TYPE(BinaryColumn);
        HANDLE_HAS_TYPE(DateColumn);
        HANDLE_HAS_TYPE(TimestampColumn);
        HANDLE_HAS_TYPE(ArrayColumn);
        HANDLE_HAS_TYPE(JsonColumn);
        HANDLE_HAS_TYPE(MapColumn);
        HANDLE_HAS_TYPE(StructColumn);

        return Status::NotSupported("unsupported operation for type: " + array_elements.get_name());
    }

    // array is non-nullable.
    static StatusOr<ColumnPtr> _array_has_non_nullable(const ArrayColumn& array, const ArrayColumn& arg) {
        bool nullable_element = false;
        bool nullable_target = false;
        bool const_target = false;
        ColumnPtr targets_holder;

        const UInt32Column& array_offsets = array.offsets();
        const UInt32Column& target_offsets = arg.offsets();
        const Column* elements = &array.elements();
        const Column* targets = &arg.elements();

        if (auto nullable = dynamic_cast<const NullableColumn*>(elements); nullable != nullptr) {
            // If this nullable column does NOT contains any NULL, process it as non-nullable column.
            nullable_element = nullable->has_null();
            elements = nullable->has_null() ? elements : nullable->data_column().get();
        }
        if (auto nullable = dynamic_cast<const NullableColumn*>(targets); nullable != nullptr) {
            nullable_target = nullable->has_null();
            targets = nullable->has_null() ? targets : nullable->data_column().get();
        }
        if (targets->only_null() && !nullable_element) {
            auto result = UInt8Column::create();
            result->resize(array.size());
            return result;
        }
        // Expand Only-Null column.
        if (targets->only_null()) {
            auto data = down_cast<const NullableColumn*>(elements)->data_column()->clone_empty();
            targets_holder = NullableColumn::create(std::move(data), NullColumn::create());
            (void)targets_holder->append_nulls(array.size());
            targets = targets_holder.get();
            nullable_target = true;
        }
        if (targets->is_constant()) {
            const_target = true;
        }

        CHECK(!(const_target && nullable_target));

        if (nullable_element && nullable_target) {
            return _array_has<true, true, false>(*elements, array_offsets, *targets, target_offsets);
        } else if (nullable_element) {
            return const_target ? _array_has<true, false, true>(*elements, array_offsets, *targets, target_offsets)
                                : _array_has<true, false, false>(*elements, array_offsets, *targets, target_offsets);
        } else if (nullable_target) {
            return _array_has<false, true, false>(*elements, array_offsets, *targets, target_offsets);
        } else {
            return const_target ? _array_has<false, false, true>(*elements, array_offsets, *targets, target_offsets)
                                : _array_has<false, false, false>(*elements, array_offsets, *targets, target_offsets);
        }
    }

    static const NullColumnPtr merge_nullcolum(const NullableColumn* a, const NullableColumn* b) {
        if (a && b == nullptr) {
            return a->null_column();
        }
        if (b && a == nullptr) {
            return b->null_column();
        }
        DCHECK_EQ(a->size(), b->size());
        return FunctionHelper::union_null_column(a->null_column(), b->null_column());
    }

    static StatusOr<ColumnPtr> _array_has_generic(const Column& array, const Column& target) {
        DCHECK_EQ(array.size(), target.size());

        const ArrayColumn* array_col = nullptr;
        const NullableColumn* array_nullable = nullptr;
        if (array.is_nullable()) {
            array_nullable = down_cast<const NullableColumn*>(&array);
            array_col = down_cast<const ArrayColumn*>(array_nullable->data_column().get());
            if (!array_nullable->has_null()) {
                array_nullable = nullptr;
            }
        } else {
            array_col = down_cast<const ArrayColumn*>(&array);
        }

        const ArrayColumn* target_col = nullptr;
        const NullableColumn* target_nullable = nullptr;
        if (target.is_nullable()) {
            target_nullable = down_cast<const NullableColumn*>(&target);
            target_col = down_cast<const ArrayColumn*>(target_nullable->data_column().get());
            if (!target_nullable->has_null()) {
                target_nullable = nullptr;
            }
        } else {
            target_col = down_cast<const ArrayColumn*>(&target);
        }

        if (array_nullable == nullptr && target_nullable == nullptr) {
            return _array_has_non_nullable(*array_col, *target_col);
        }

        ASSIGN_OR_RETURN(auto result, _array_has_non_nullable(*array_col, *target_col))
        DCHECK_EQ(array_col->size(), result->size());
        return NullableColumn::create(std::move(result), merge_nullcolum(array_nullable, target_nullable));
    }
};

StatusOr<ColumnPtr> ArrayFunctions::array_contains([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL({columns[0]});
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = columns[1];                                                                      // element

    return ArrayContainsImpl<false, UInt8Column>::evaluate(*arg0, *arg1);
}

StatusOr<ColumnPtr> ArrayFunctions::array_position([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL({columns[0]});
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = columns[1];                                                                      // element

    return ArrayContainsImpl<true, Int32Column>::evaluate(*arg0, *arg1);
}

StatusOr<ColumnPtr> ArrayFunctions::array_contains_any([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = ColumnHelper::unpack_and_duplicate_const_column(columns[1]->size(), columns[1]); // element

    return ArrayHasImpl<true>::evaluate(*arg0, *arg1);
}

StatusOr<ColumnPtr> ArrayFunctions::array_contains_all([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = ColumnHelper::unpack_and_duplicate_const_column(columns[1]->size(), columns[1]); // element

    return ArrayHasImpl<false>::evaluate(*arg0, *arg1);
}

// cannot be called anymore
StatusOr<ColumnPtr> ArrayFunctions::array_map([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return nullptr;
}

StatusOr<ColumnPtr> ArrayFunctions::array_filter(FunctionContext* context, const Columns& columns) {
    return ArrayFilter::process(context, columns);
}

StatusOr<ColumnPtr> ArrayFunctions::all_match(FunctionContext* context, const Columns& columns) {
    return ArrayMatch<false>::process(context, columns);
}

StatusOr<ColumnPtr> ArrayFunctions::any_match(FunctionContext* context, const Columns& columns) {
    return ArrayMatch<true>::process(context, columns);
}

StatusOr<ColumnPtr> ArrayFunctions::concat(FunctionContext* ctx, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto num_rows = columns[0]->size();

    // compute nulls
    NullColumnPtr nulls;
    for (auto& column : columns) {
        if (column->has_null()) {
            auto nullable_column = down_cast<NullableColumn*>(column.get());
            if (nulls == nullptr) {
                nulls = std::static_pointer_cast<NullColumn>(nullable_column->null_column()->clone_shared());
            } else {
                ColumnHelper::or_two_filters(num_rows, nulls->get_data().data(),
                                             nullable_column->null_column()->get_data().data());
            }
        }
    }

    // collect all array columns
    std::vector<ArrayColumn::Ptr> array_columns;
    for (auto& column : columns) {
        if (column->is_nullable()) {
            auto nullable_column = down_cast<NullableColumn*>(column.get());
            array_columns.emplace_back(std::static_pointer_cast<ArrayColumn>(nullable_column->data_column()));
        } else if (column->is_constant()) {
            // NOTE: I'm not sure if there will be const array, just to be safe
            array_columns.emplace_back(std::static_pointer_cast<ArrayColumn>(
                    ColumnHelper::unpack_and_duplicate_const_column(num_rows, column)));
        } else {
            array_columns.emplace_back(std::static_pointer_cast<ArrayColumn>(column));
        }
    }
    auto dst = array_columns[0]->clone_empty();
    auto dst_array = down_cast<ArrayColumn*>(dst.get());
    uint32_t dst_offset = 0;
    if (nulls != nullptr) {
        auto& null_vec = nulls->get_data();
        for (int row = 0; row < num_rows; ++row) {
            if (!null_vec[row]) {
                for (auto& column : array_columns) {
                    auto off_size = column->get_element_offset_size(row);
                    dst_array->elements_column()->append(column->elements(), off_size.first, off_size.second);
                    dst_offset += off_size.second;
                }
            }
            dst_array->offsets_column()->append(dst_offset);
        }
    } else {
        for (int row = 0; row < num_rows; ++row) {
            for (auto& column : array_columns) {
                auto off_size = column->get_element_offset_size(row);
                dst_array->elements_column()->append(column->elements(), off_size.first, off_size.second);
                dst_offset += off_size.second;
            }
            dst_array->offsets_column()->append(dst_offset);
        }
    }
    if (nulls == nullptr) {
        return std::move(dst);
    } else {
        return NullableColumn::create(std::move(dst), std::move(nulls));
    }
}

template <bool with_length>
void _array_slice_item(ArrayColumn* column, size_t index, ArrayColumn* dest_column, int64_t offset, int64_t length) {
    auto& dest_offsets = dest_column->offsets_column()->get_data();
    if (!offset) {
        dest_offsets.emplace_back(dest_offsets.back());
        return;
    }

    Datum v = column->get(index);
    const auto& items = v.get<DatumArray>();

    if (offset > 0) {
        // because offset start with 1.
        --offset;
    } else {
        offset += items.size();
    }

    auto& dest_data_column = dest_column->elements_column();
    int64_t end;
    if constexpr (with_length) {
        end = std::max((int64_t)0, std::min((int64_t)items.size(), (offset + length)));
    } else {
        end = items.size();
    }
    offset = (offset > 0 ? offset : 0);
    for (size_t i = offset; i < end; ++i) {
        if (items[i].is_null()) {
            dest_data_column->append_nulls(1);
        } else {
            dest_data_column->append_datum(items[i]);
        }
    }

    // Protect when length < 0.
    auto offset_delta = ((end < offset) ? 0 : end - offset);
    dest_offsets.emplace_back(dest_offsets.back() + offset_delta);
}

StatusOr<ColumnPtr> ArrayFunctions::array_slice(FunctionContext* ctx, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    size_t chunk_size = columns[0]->size();
    ColumnPtr src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]);
    ColumnPtr dest_column = src_column->clone_empty();

    bool is_nullable = false;
    bool has_null = false;
    NullColumnPtr null_result = nullptr;

    ArrayColumn* array_column = nullptr;
    if (columns[0]->is_nullable()) {
        is_nullable = true;
        has_null = (columns[0]->has_null() || has_null);

        const auto* src_nullable_column = down_cast<const NullableColumn*>(columns[0].get());
        array_column = down_cast<ArrayColumn*>(src_nullable_column->data_column().get());
        null_result = NullColumn::create(*src_nullable_column->null_column());
    } else {
        array_column = down_cast<ArrayColumn*>(src_column.get());
    }

    Int64Column* offset_column = nullptr;
    if (columns[1]->is_nullable()) {
        is_nullable = true;
        has_null = (columns[1]->has_null() || has_null);

        const auto* src_nullable_column = down_cast<const NullableColumn*>(columns[1].get());
        offset_column = down_cast<Int64Column*>(src_nullable_column->data_column().get());
        if (null_result) {
            null_result = FunctionHelper::union_null_column(null_result, src_nullable_column->null_column());
        } else {
            null_result = NullColumn::create(*src_nullable_column->null_column());
        }
    } else {
        offset_column =
                down_cast<Int64Column*>(ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[1]).get());
    }

    Int64Column* length_column = nullptr;
    // length_column is provided.
    if (columns.size() > 2) {
        if (columns[2]->is_nullable()) {
            is_nullable = true;
            has_null = (columns[2]->has_null() || has_null);

            const auto* src_nullable_column = down_cast<const NullableColumn*>(columns[2].get());
            length_column = down_cast<Int64Column*>(src_nullable_column->data_column().get());
            if (null_result) {
                null_result = FunctionHelper::union_null_column(null_result, src_nullable_column->null_column());
            } else {
                null_result = NullColumn::create(*src_nullable_column->null_column());
            }
        } else {
            length_column = down_cast<Int64Column*>(
                    ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[2]).get());
        }
    }

    ArrayColumn* dest_data_column = nullptr;
    if (columns[0]->is_nullable()) {
        auto& dest_nullable_column = down_cast<NullableColumn&>(*dest_column);
        dest_data_column = down_cast<ArrayColumn*>(dest_nullable_column.data_column().get());
        auto& dest_null_data = dest_nullable_column.null_column_data();

        dest_null_data = null_result->get_data();
        dest_nullable_column.set_has_null(has_null);
    } else {
        dest_data_column = down_cast<ArrayColumn*>(dest_column.get());
    }

    if (columns.size() > 2) {
        for (size_t i = 0; i < chunk_size; i++) {
            _array_slice_item<true>(array_column, i, dest_data_column, offset_column->get(i).get_int64(),
                                    length_column->get(i).get_int64());
        }
    } else {
        for (size_t i = 0; i < chunk_size; i++) {
            _array_slice_item<false>(array_column, i, dest_data_column, offset_column->get(i).get_int64(), 0);
        }
    }

    if (is_nullable) {
        if (columns[0]->is_nullable()) {
            return dest_column;
        } else {
            return NullableColumn::create(dest_column, null_result);
        }
    } else {
        return dest_column;
    }
}

// unpack array column, return: null_column, element_column, offset_column
static inline std::tuple<NullColumnPtr, Column*, const UInt32Column*> unpack_array_column(const ColumnPtr& input) {
    NullColumnPtr array_null = nullptr;
    ArrayColumn* array_col = nullptr;

    auto array = ColumnHelper::unpack_and_duplicate_const_column(input->size(), input);
    if (array->is_nullable()) {
        auto nullable = down_cast<NullableColumn*>(array.get());
        array_col = down_cast<ArrayColumn*>(nullable->data_column().get());
        array_null = NullColumn::create(*nullable->null_column());
    } else {
        array_null = NullColumn::create(input->size(), 0);
        array_col = down_cast<ArrayColumn*>(array.get());
    }

    const UInt32Column* offsets = &array_col->offsets();
    Column* elements = array_col->elements_column().get();

    return {array_null, elements, offsets};
}

StatusOr<ColumnPtr> ArrayFunctions::array_distinct_any_type(FunctionContext* ctx, const Columns& columns) {
    DCHECK_EQ(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto [array_null, elements, offsets] = unpack_array_column(columns[0]);
    auto* offsets_ptr = offsets->get_data().data();
    auto* row_nulls = array_null->get_data().data();

    auto result_elements = elements->clone_empty();
    auto result_offsets = UInt32Column::create();
    result_offsets->reserve(offsets->size());
    result_offsets->append(0);

    phmap::flat_hash_set<uint32_t> sets;

    uint32_t hash[elements->size()];
    memset(hash, 0, elements->size() * sizeof(uint32_t));
    elements->fnv_hash(hash, 0, elements->size());

    size_t rows = columns[0]->size();
    for (auto i = 0; i < rows; i++) {
        size_t offset = offsets_ptr[i];
        int64_t array_size = offsets_ptr[i + 1] - offsets_ptr[i];

        if (row_nulls[i] == 1) {
            // append offsets directly
        } else if (array_size <= 1) {
            for (size_t j = 0; j < array_size; j++) {
                result_elements->append(*elements, offset + j, 1);
            }
        } else {
            // put first
            result_elements->append(*elements, offset, 1);

            sets.clear();
            sets.emplace(hash[offset]);

            for (size_t j = 1; j < array_size; j++) {
                auto elements_idx = offset + j;
                // hash check
                if (!sets.contains(hash[elements_idx])) {
                    result_elements->append(*elements, elements_idx, 1);
                    sets.emplace(hash[elements_idx]);
                    continue;
                }

                // find same hash
                bool is_contains = false;
                for (size_t k = offset; k < elements_idx; k++) {
                    if (hash[k] == hash[elements_idx] && elements->equals(k, *elements, elements_idx)) {
                        is_contains = true;
                        break;
                    }
                }

                if (!is_contains) {
                    result_elements->append(*elements, elements_idx, 1);
                }
            }
        }

        result_offsets->append(result_elements->size());
    }

    return NullableColumn::create(ArrayColumn::create(std::move(result_elements), result_offsets), array_null);
}

StatusOr<ColumnPtr> ArrayFunctions::array_reverse_any_types(FunctionContext* ctx, const Columns& columns) {
    DCHECK_EQ(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto [array_null, elements, offsets] = unpack_array_column(columns[0]);
    auto* offsets_ptr = offsets->get_data().data();
    auto* row_nulls = array_null->get_data().data();

    auto result_elements = elements->clone_empty();
    auto result_offsets = UInt32Column::create();
    result_offsets->reserve(offsets->size());
    result_offsets->append(0);

    size_t rows = columns[0]->size();
    for (auto i = 0; i < rows; i++) {
        size_t offset = offsets_ptr[i];
        int64_t array_size = offsets_ptr[i + 1] - offsets_ptr[i];

        if (row_nulls[i] == 0) {
            for (int64_t j = array_size - 1; j >= 0; j--) {
                result_elements->append(*elements, offset + j, 1);
            }
        }

        result_offsets->append(result_elements->size());
    }

    return NullableColumn::create(ArrayColumn::create(std::move(result_elements), std::move(result_offsets)),
                                  array_null);
}

inline static void nestloop_intersect(uint8_t* hits, const Column* base, size_t base_start, size_t base_end,
                                      const Column* cmp, size_t cmp_start, size_t cmp_end) {
    for (size_t base_ele_idx = base_start; base_ele_idx < base_end; base_ele_idx++) {
        if (hits[base_ele_idx] == 0) {
            continue;
        }

        size_t cmp_idx = cmp_start;
        for (; cmp_idx < cmp_end; cmp_idx++) {
            if (base->equals(base_ele_idx, *cmp, cmp_idx)) {
                break;
            }
        }

        if (cmp_idx >= cmp_end) {
            hits[base_ele_idx] = 0;
        }
    }
}

StatusOr<ColumnPtr> ArrayFunctions::array_intersect_any_type(FunctionContext* ctx, const Columns& columns) {
    DCHECK_LE(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    size_t rows = columns[0]->size();
    size_t usage = columns[0]->memory_usage();
    ColumnPtr base_col = columns[0];
    int base_idx = 0;

    auto nulls = NullColumn::create(rows, 0);
    // find minimum column
    for (size_t i = 0; i < columns.size(); i++) {
        if (columns[i]->memory_usage() < usage) {
            base_col = columns[i];
            base_idx = i;
            usage = columns[i]->memory_usage();
        }

        FunctionHelper::union_produce_nullable_column(columns[i], &nulls);
    }

    // do distinct first
    auto distinct_col = array_distinct_any_type(ctx, {base_col});
    DCHECK(distinct_col.ok());

    auto* dis_array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(distinct_col.value().get()));

    auto& base_elements = dis_array_col->elements_column();
    auto* base_offsets = &dis_array_col->offsets();
    auto* base_offsets_ptr = base_offsets->get_data().data();
    auto* nulls_ptr = nulls->get_data().data();

    Filter filter;
    filter.resize(base_elements->size(), 1);
    auto* hits = filter.data();

    // mark intersect filter
    for (size_t col_idx = 0; col_idx < columns.size(); col_idx++) {
        if (col_idx == base_idx) {
            continue;
        }

        auto [_2, cmp_elements, cmp_offsets] = unpack_array_column(columns[col_idx]);
        auto* cmp_offsets_ptr = cmp_offsets->get_data().data();

        for (size_t row_idx = 0; row_idx < rows; row_idx++) {
            if (nulls_ptr[row_idx] == 1) {
                continue;
            }

            size_t base_start = base_offsets_ptr[row_idx];
            size_t base_end = base_offsets_ptr[row_idx + 1];

            size_t cmp_start = cmp_offsets_ptr[row_idx];
            size_t cmp_end = cmp_offsets_ptr[row_idx + 1];

            nestloop_intersect(hits, base_elements.get(), base_start, base_end, cmp_elements, cmp_start, cmp_end);
        }
    }

    // result column
    auto result_offsets = UInt32Column::create();
    base_elements->filter(filter);

    result_offsets->append(0);
    uint32_t pre_offset = 0;
    for (size_t row_idx = 0; row_idx < rows; row_idx++) {
        size_t base_start = base_offsets_ptr[row_idx];
        size_t size = base_offsets_ptr[row_idx + 1] - base_start;

        auto count = SIMD::count_nonzero(hits + base_start, size);
        pre_offset += count;
        result_offsets->append(pre_offset);
    }

    return NullableColumn::create(ArrayColumn::create(base_elements, result_offsets), nulls);
}
} // namespace starrocks
