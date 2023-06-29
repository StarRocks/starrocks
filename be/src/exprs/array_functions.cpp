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
    static StatusOr<ColumnPtr> evaluate(const ColumnPtr array, const ColumnPtr element) {
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
        if (auto nullable = dynamic_cast<const NullableColumn*>(array.get()); nullable != nullptr) {
            auto array_col = down_cast<const ArrayColumn*>(nullable->data_column().get());
            ASSIGN_OR_RETURN(auto result, _array_remove_non_nullable(*array_col, *target));
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
            return ConstColumn::create(std::move(arr_col_h));
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
            size_t offset = offsets[i];
            size_t array_size = offsets[i + 1] - offsets[i];
            if constexpr (nullable) {
                DCHECK(null_data != nullptr);
                if (null_data[offset]) {
                    continue;
                }
            }
            RunTimeCppType<TYPE> cum_sum{};
            if constexpr (element_nullable) {
                if (element_null_data[offset]) {
                    // skip null
                } else {
                    cum_sum += element_data[offset];
                }
            } else {
                cum_sum += element_data[offset];
            }

            for (int j = offset + 1; j < offset + array_size; ++j) {
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
        if (auto nullable = dynamic_cast<const NullableColumn*>(&array); nullable != nullptr) {
            auto array_col = down_cast<const ArrayColumn*>(nullable->data_column().get());
            ASSIGN_OR_RETURN(auto result, _array_contains_non_nullable(*array_col, target));
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
        using ValueType = std::conditional_t<std::is_same_v<ArrayColumn, ElementColumn>, uint8_t,
                                             typename ElementColumn::ValueType>;
        [[maybe_unused]] auto elements_ptr = (const ValueType*)(elements.raw_data());
        [[maybe_unused]] auto targets_ptr = (const ValueType*)(targets.raw_data());

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
                if constexpr (std::is_same_v<ArrayColumn, ElementColumn> || std::is_same_v<JsonColumn, ElementColumn>) {
                    found = (elements.compare_at(j, i, targets, -1) == 0);
                } else {
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

        ASSIGN_OR_RETURN(auto result, _array_has_non_nullable(*array_col, *target_col));
        DCHECK_EQ(array_nullable->size(), result->size());
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

class ArrayArithmeticImpl {
public:
    using ArithmeticType = typename ArrayFunctions::ArithmeticType;

    template <ArithmeticType type, LogicalType value_type, LogicalType sum_result_type, LogicalType avg_result_type,
              bool has_null, typename ElementColumn>
    static StatusOr<ColumnPtr> _sum_and_avg(const ElementColumn& elements, const UInt32Column& offsets,
                                            const NullColumn::Container* null_elements,
                                            std::vector<uint8_t>* null_ptr) {
        const size_t num_array = offsets.size() - 1;
        auto offsets_ptr = offsets.get_data().data();

        using ValueType = RunTimeCppType<value_type>;
        using ResultColumnType = std::conditional_t<type == ArithmeticType::SUM, RunTimeColumnType<sum_result_type>,
                                                    RunTimeColumnType<avg_result_type>>;

        using ResultType = std::conditional_t<
                type == ArithmeticType::SUM, RunTimeCppType<sum_result_type>,
                std::conditional_t<std::is_same_v<DateColumn, ResultColumnType>, double,
                                   std::conditional_t<std::is_same_v<TimestampColumn, ResultColumnType>, double,
                                                      RunTimeCppType<avg_result_type>>>>;

        auto result_column = ResultColumnType::create();
        result_column->reserve(num_array);

        auto elements_ptr = (const ValueType*)(elements.raw_data());

        for (size_t i = 0; i < num_array; i++) {
            size_t offset = offsets_ptr[i];
            size_t array_size = offsets_ptr[i + 1] - offsets_ptr[i];
            ResultType sum{};

            bool has_data = false;
            for (size_t j = 0; j < array_size; j++) {
                if constexpr (has_null) {
                    if ((*null_elements)[offset + j] != 0) {
                        continue;
                    }
                }

                has_data = true;
                auto& value = elements_ptr[offset + j];
                if constexpr (lt_is_datetime<value_type>) {
                    sum += value.to_unix_second();
                } else if constexpr (lt_is_date<value_type>) {
                    sum += value.julian();
                } else {
                    sum += value;
                }
            }

            if (has_data) {
                if constexpr (lt_is_decimalv2<value_type>) {
                    if constexpr (type == ArithmeticType::SUM) {
                        result_column->append(sum);
                    } else {
                        result_column->append(sum / DecimalV2Value(array_size, 0));
                    }
                } else if constexpr (lt_is_arithmetic<value_type> || lt_is_decimal<value_type>) {
                    if constexpr (type == ArithmeticType::SUM) {
                        result_column->append(sum);
                    } else {
                        result_column->append(sum / array_size);
                    }
                } else if constexpr (lt_is_datetime<value_type>) {
                    static_assert(type == ArithmeticType::AVG);
                    TimestampValue value;
                    value.from_unix_second(sum / array_size);
                    result_column->append(value);
                } else if constexpr (lt_is_date<value_type>) {
                    static_assert(type == ArithmeticType::AVG);
                    DateValue value;
                    value._julian = sum / array_size;
                    result_column->append(value);
                } else {
                    LOG(ERROR) << "unhandled types other than arithmetic/time/decimal for sum and avg";
                    DCHECK(false) << "other types than arithmetic/time/decimal is not support sum "
                                     "and avg";
                    result_column->append_default();
                }
            } else {
                result_column->append_default();
                (*null_ptr)[i] = 1;
            }
        }

        return result_column;
    }

    template <bool is_min, ArithmeticType type, LogicalType value_type, bool has_null, typename ElementColumn>
    static StatusOr<ColumnPtr> _min_and_max(const ElementColumn& elements, const UInt32Column& offsets,
                                            const NullColumn::Container* null_elements,
                                            std::vector<uint8_t>* null_ptr) {
        const size_t num_array = offsets.size() - 1;
        auto offsets_ptr = offsets.get_data().data();

        using ValueType = RunTimeCppType<value_type>;
        using ResultColumnType = RunTimeColumnType<value_type>;
        using ResultType = ValueType;

        auto result_column = ResultColumnType::create();
        result_column->reserve(num_array);

        auto elements_ptr = (const ValueType*)(elements.raw_data());

        for (size_t i = 0; i < num_array; i++) {
            size_t offset = offsets_ptr[i];
            size_t array_size = offsets_ptr[i + 1] - offsets_ptr[i];

            if (array_size > 0) {
                ResultType result;

                size_t index;
                if constexpr (!lt_is_string<value_type>) {
                    if constexpr (is_min) {
                        result = RunTimeTypeLimits<value_type>::max_value();
                    } else {
                        result = RunTimeTypeLimits<value_type>::min_value();
                    }
                    index = 0;
                } else {
                    int j = 0;
                    if constexpr (has_null) {
                        while (j < array_size && (*null_elements)[offset + j] != 0) {
                            ++j;
                        }
                    }
                    if (j < array_size) {
                        result = elements_ptr[offset + j];
                    } else {
                        result_column->append_default();
                        (*null_ptr)[i] = 1;
                        continue;
                    }

                    index = j + 1;
                }

                bool has_data = false;
                for (; index < array_size; index++) {
                    if constexpr (has_null) {
                        if ((*null_elements)[offset + index] != 0) {
                            continue;
                        }
                    }

                    has_data = true;
                    auto& value = elements_ptr[offset + index];
                    if constexpr (is_min) {
                        result = result < value ? result : value;
                    } else {
                        result = result < value ? value : result;
                    }
                }

                if constexpr (!lt_is_string<value_type>) {
                    if (has_data) {
                        result_column->append(result);
                    } else {
                        result_column->append_default();
                        (*null_ptr)[i] = 1;
                    }
                } else {
                    result_column->append(result);
                }
            } else {
                result_column->append_default();
                (*null_ptr)[i] = 1;
            }
        }

        return result_column;
    }
};

template <LogicalType column_type, bool has_null, ArrayFunctions::ArithmeticType type>
StatusOr<ColumnPtr> ArrayFunctions::_array_process_not_nullable_types(const Column* elements,
                                                                      const UInt32Column& offsets,
                                                                      const NullColumn::Container* null_elements,
                                                                      std::vector<uint8_t>* null_ptr) {
    [[maybe_unused]] auto c = down_cast<const RunTimeColumnType<column_type>*>(elements);

    // FOR ARITHEMIC TYPE (BOOLEAN, TINYINT, SMALLINT, INT, BIGINT)
    if constexpr (column_type == TYPE_BOOLEAN || column_type == TYPE_TINYINT || column_type == TYPE_SMALLINT ||
                  column_type == TYPE_INT || column_type == TYPE_BIGINT) {
        if constexpr (type == ArithmeticType::SUM || type == ArithmeticType::AVG) {
            return ArrayArithmeticImpl::template _sum_and_avg<type, column_type, TYPE_BIGINT, TYPE_DOUBLE, has_null>(
                    *c, offsets, null_elements, null_ptr);
        } else {
            static_assert(type == ArithmeticType::MIN || type == ArithmeticType::MAX);
            return ArrayArithmeticImpl::template _min_and_max<type == ArithmeticType::MIN, type, column_type, has_null>(
                    *c, offsets, null_elements, null_ptr);
        }
    } else if constexpr (column_type == TYPE_LARGEINT) {
        // FOR ARITHEMIC TYPE (LARGEINT)
        if constexpr (type == ArithmeticType::SUM || type == ArithmeticType::AVG) {
            return ArrayArithmeticImpl::template _sum_and_avg<type, TYPE_LARGEINT, TYPE_LARGEINT, TYPE_DOUBLE,
                                                              has_null>(*c, offsets, null_elements, null_ptr);
        } else {
            static_assert(type == ArithmeticType::MIN || type == ArithmeticType::MAX);
            return ArrayArithmeticImpl::template _min_and_max<type == ArithmeticType::MIN, type, TYPE_LARGEINT,
                                                              has_null>(*c, offsets, null_elements, null_ptr);
        }
    } else if constexpr (column_type == TYPE_FLOAT || column_type == TYPE_DOUBLE) {
        // FOR FLOAT TYPE (FLOAT, DOUBLE)
        if constexpr (type == ArithmeticType::SUM || type == ArithmeticType::AVG) {
            return ArrayArithmeticImpl::template _sum_and_avg<type, column_type, TYPE_DOUBLE, TYPE_DOUBLE, has_null>(
                    *c, offsets, null_elements, null_ptr);
        } else {
            static_assert(type == ArithmeticType::MIN || type == ArithmeticType::MAX);
            return ArrayArithmeticImpl::template _min_and_max<type == ArithmeticType::MIN, type, column_type, has_null>(
                    *c, offsets, null_elements, null_ptr);
        }
    } else if constexpr (column_type == TYPE_DECIMALV2) {
        // FOR DECIMALV2 TYPE
        if constexpr (type == ArithmeticType::SUM || type == ArithmeticType::AVG) {
            return ArrayArithmeticImpl::template _sum_and_avg<type, TYPE_DECIMALV2, TYPE_DECIMALV2, TYPE_DECIMALV2,
                                                              has_null>(*c, offsets, null_elements, null_ptr);
        } else {
            static_assert(type == ArithmeticType::MIN || type == ArithmeticType::MAX);
            return ArrayArithmeticImpl::template _min_and_max<type == ArithmeticType::MIN, type, TYPE_DECIMALV2,
                                                              has_null>(*c, offsets, null_elements, null_ptr);
        }
    } else if constexpr (column_type == TYPE_DATE || column_type == TYPE_DATETIME || column_type == TYPE_VARCHAR ||
                         column_type == TYPE_CHAR) {
        // FOR DATE/DATETIME TYPE
        if constexpr (type == ArithmeticType::SUM || type == ArithmeticType::AVG) {
            LOG(ERROR) << "sum and avg not support date/datetime/char/varchar";
            DCHECK(false) << "sum and avg not support date/datetime/char/varchar";
            return nullptr;
        } else {
            static_assert(type == ArithmeticType::MIN || type == ArithmeticType::MAX);
            return ArrayArithmeticImpl::template _min_and_max<type == ArithmeticType::MIN, type, column_type, has_null>(
                    *c, offsets, null_elements, null_ptr);
        }
    } else {
        LOG(ERROR) << "unhandled column type: " << typeid(*elements).name();
        DCHECK(false) << "unhandled column type: " << typeid(*elements).name();
        auto all_null = ColumnHelper::create_const_null_column(elements->size());
        return all_null;
    }
}

template <LogicalType column_type, ArrayFunctions::ArithmeticType type>
StatusOr<ColumnPtr> ArrayFunctions::_array_process_not_nullable(const Column* raw_array_column,
                                                                std::vector<uint8_t>* null_ptr) {
    const auto& array_column = down_cast<const ArrayColumn&>(*raw_array_column);
    const UInt32Column& offsets = array_column.offsets();
    const Column* elements = &array_column.elements();

    const NullColumn::Container* null_elements = nullptr;

    bool has_null = elements->has_null();
    if (has_null) {
        null_elements = &(down_cast<const NullableColumn*>(elements)->null_column()->get_data());
    }

    if (auto nullable = dynamic_cast<const NullableColumn*>(elements); nullable != nullptr) {
        elements = nullable->data_column().get();
    }

    if (has_null) {
        return ArrayFunctions::template _array_process_not_nullable_types<column_type, true, type>(
                elements, offsets, null_elements, null_ptr);
    } else {
        return ArrayFunctions::template _array_process_not_nullable_types<column_type, false, type>(
                elements, offsets, null_elements, null_ptr);
    }
}

template <LogicalType column_type, ArrayFunctions::ArithmeticType type>
StatusOr<ColumnPtr> ArrayFunctions::array_arithmetic(const Columns& columns) {
    DCHECK_EQ(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const ColumnPtr& array_column = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]);
    const auto& raw_array_column = *array_column;

    if (auto nullable = dynamic_cast<const NullableColumn*>(&raw_array_column); nullable != nullptr) {
        auto array_col = down_cast<const ArrayColumn*>(nullable->data_column().get());
        auto null_column = NullColumn::create(*nullable->null_column());
        auto result = ArrayFunctions::template _array_process_not_nullable<column_type, type>(array_col,
                                                                                              &null_column->get_data());
        RETURN_IF_ERROR(result);

        DCHECK_EQ(nullable->size(), result.value()->size());
        return NullableColumn::create(std::move(result.value()), null_column);
    } else {
        auto null_column = NullColumn::create();
        null_column->resize(raw_array_column.size());
        auto result = ArrayFunctions::template _array_process_not_nullable<column_type, type>(&raw_array_column,
                                                                                              &null_column->get_data());
        RETURN_IF_ERROR(result);
        return NullableColumn::create(std::move(result.value()), null_column);
    }
}

template <LogicalType type>
StatusOr<ColumnPtr> ArrayFunctions::array_sum(const Columns& columns) {
    return ArrayFunctions::template array_arithmetic<type, ArithmeticType::SUM>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_sum_boolean([[maybe_unused]] FunctionContext* context,
                                                      const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_BOOLEAN>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_sum_tinyint([[maybe_unused]] FunctionContext* context,
                                                      const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_TINYINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_sum_smallint([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_SMALLINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_sum_int([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_INT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_sum_bigint([[maybe_unused]] FunctionContext* context,
                                                     const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_BIGINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_sum_largeint([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_LARGEINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_sum_float([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_FLOAT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_sum_double([[maybe_unused]] FunctionContext* context,
                                                     const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_DOUBLE>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_sum_decimalv2([[maybe_unused]] FunctionContext* context,
                                                        const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_DECIMALV2>(columns);
}

template <LogicalType type>
StatusOr<ColumnPtr> ArrayFunctions::array_avg(const Columns& columns) {
    return ArrayFunctions::template array_arithmetic<type, ArithmeticType::AVG>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_avg_boolean([[maybe_unused]] FunctionContext* context,
                                                      const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_BOOLEAN>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_avg_tinyint([[maybe_unused]] FunctionContext* context,
                                                      const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_TINYINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_avg_smallint([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_SMALLINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_avg_int([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_INT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_avg_bigint([[maybe_unused]] FunctionContext* context,
                                                     const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_BIGINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_avg_largeint([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_LARGEINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_avg_float([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_FLOAT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_avg_double([[maybe_unused]] FunctionContext* context,
                                                     const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_DOUBLE>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_avg_decimalv2([[maybe_unused]] FunctionContext* context,
                                                        const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_DECIMALV2>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_avg_date([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_DATE>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_avg_datetime([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_DATETIME>(columns);
}

template <LogicalType type>
StatusOr<ColumnPtr> ArrayFunctions::array_min(const Columns& columns) {
    return ArrayFunctions::template array_arithmetic<type, ArithmeticType::MIN>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_min_boolean([[maybe_unused]] FunctionContext* context,
                                                      const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_BOOLEAN>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_min_tinyint([[maybe_unused]] FunctionContext* context,
                                                      const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_TINYINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_min_smallint([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_SMALLINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_min_int([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_INT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_min_bigint([[maybe_unused]] FunctionContext* context,
                                                     const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_BIGINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_min_largeint([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_LARGEINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_min_float([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_FLOAT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_min_double([[maybe_unused]] FunctionContext* context,
                                                     const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_DOUBLE>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_min_decimalv2([[maybe_unused]] FunctionContext* context,
                                                        const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_DECIMALV2>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_min_date([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_DATE>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_min_datetime([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_DATETIME>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_min_varchar([[maybe_unused]] FunctionContext* context,
                                                      const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_VARCHAR>(columns);
}

template <LogicalType type>
StatusOr<ColumnPtr> ArrayFunctions::array_max(const Columns& columns) {
    return ArrayFunctions::template array_arithmetic<type, ArithmeticType::MAX>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_max_boolean([[maybe_unused]] FunctionContext* context,
                                                      const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_BOOLEAN>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_max_tinyint([[maybe_unused]] FunctionContext* context,
                                                      const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_TINYINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_max_smallint([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_SMALLINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_max_int([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_INT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_max_bigint([[maybe_unused]] FunctionContext* context,
                                                     const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_BIGINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_max_largeint([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_LARGEINT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_max_float([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_FLOAT>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_max_double([[maybe_unused]] FunctionContext* context,
                                                     const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_DOUBLE>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_max_decimalv2([[maybe_unused]] FunctionContext* context,
                                                        const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_DECIMALV2>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_max_date([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_DATE>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_max_datetime([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_DATETIME>(columns);
}

StatusOr<ColumnPtr> ArrayFunctions::array_max_varchar([[maybe_unused]] FunctionContext* context,
                                                      const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_VARCHAR>(columns);
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

} // namespace starrocks
