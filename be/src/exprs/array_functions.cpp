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

#include <memory>

#include "column/array_column.h"
#include "column/column_hash.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "simd/simd.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_fwd_decl.h"
#include "util/raw_container.h"

namespace starrocks {

StatusOr<ColumnPtr> ArrayFunctions::array_length([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const size_t num_rows = columns[0]->size();
    const auto* col_array = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[0].get()));
    if (columns[0]->is_constant()) {
        auto col_result = Int32Column::create();
        col_result->append(col_array->offsets().get_data().data()[1]);
        auto const_column = ConstColumn::create(std::move(col_result), num_rows);
        return const_column;
    } else {
        const auto* arg0 = columns[0].get();
        auto col_result = Int32Column::create();
        raw::make_room(&col_result->get_data(), num_rows);
        DCHECK_EQ(col_array->size(), col_result->size());

        const uint32_t* offsets = col_array->offsets().get_data().data();
        int32_t* p = col_result->get_data().data();
        for (size_t i = 0; i < num_rows; i++) {
            p[i] = offsets[i + 1] - offsets[i];
        }

        if (arg0->has_null()) {
            // Copy null flags.
            return NullableColumn::create(std::move(col_result),
                                          down_cast<const NullableColumn*>(arg0)->null_column()->clone());
        } else {
            return col_result;
        }
    }
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

        auto* targets_col = &targets;
        if constexpr (ConstTarget) {
            targets_col = down_cast<const ConstColumn*>(&targets)->data_column().get();
        }

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
                    if constexpr (ConstTarget) {
                        found = elements.equals(offset + j, *targets_col, 0);
                    } else {
                        found = elements.equals(offset + j, targets, i);
                    }
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
#undef HANDLE_ELEMENT_TYPE

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

        return _array_remove_non_nullable(down_cast<const ArrayColumn&>(*array), *target);
    }
};

template <LogicalType TYPE>
struct ArrayCumSumImpl {
public:
    static StatusOr<ColumnPtr> evaluate(const ColumnPtr& col) {
        if (col->is_constant()) {
            const auto* input = down_cast<const ConstColumn*>(col.get());
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
    template <bool NullableElement, bool NullableTarget, bool ConstElement, bool ConstTarget, typename ElementColumn,
              typename TargetColumn>
    static StatusOr<ColumnPtr> _process(const ElementColumn& elements, const UInt32Column& offsets,
                                        const TargetColumn& targets, const NullColumn::Container* null_map_elements,
                                        const NullColumn::Container* null_map_targets) {
        auto result = ReturnType::create();
        if constexpr (ConstElement && ConstTarget) {
            // if both element and target column are const, we only compute once here and generate ConstColumn with target size outside.
            result->resize(1);
        } else {
            result->resize(targets.size());
        }
        const size_t result_size = result->size();

        auto* result_ptr = result->get_data().data();

        using ValueType = std::conditional_t<std::is_same_v<ArrayColumn, ElementColumn> ||
                                                     std::is_same_v<MapColumn, ElementColumn> ||
                                                     std::is_same_v<StructColumn, ElementColumn>,
                                             uint8_t, typename ElementColumn::ValueType>;

        auto offsets_ptr = offsets.get_data().data();

        [[maybe_unused]] auto is_null = [](const NullColumn::Container* null_map, size_t idx) -> bool {
            return (*null_map)[idx] != 0;
        };

        auto* targets_col = &targets;
        if constexpr (ConstTarget) {
            targets_col = down_cast<const ConstColumn*>(&targets)->data_column().get();
        }

        for (size_t i = 0; i < result_size; i++) {
            size_t offset = ConstElement ? offsets_ptr[0] : offsets_ptr[i];
            size_t array_size = ConstElement ? offsets_ptr[1] - offsets_ptr[0] : offsets_ptr[i + 1] - offsets_ptr[i];

            if constexpr (!NullableElement && NullableTarget) {
                if (is_null(null_map_targets, i)) {
                    result_ptr[i] = 0;
                    continue;
                }
            }
            uint8_t found = 0;
            size_t position = 0;
            for (size_t j = 0; j < array_size; j++) {
                if constexpr (NullableElement && !NullableTarget) {
                    if (is_null(null_map_elements, offset + j)) {
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
                    if (ConstTarget) {
                        found = elements.equals(offset + j, *targets_col, 0);
                    } else {
                        found = elements.equals(offset + j, targets, i);
                    }
                } else if constexpr (ConstTarget) {
                    auto elements_ptr = (const ValueType*)(elements.raw_data());
                    auto targets_ptr = (const ValueType*)(targets.raw_data());
                    auto& first_target = *targets_ptr;
                    found = (elements_ptr[offset + j] == first_target);
                } else {
                    auto elements_ptr = (const ValueType*)(elements.raw_data());
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

    template <bool NullableElement, bool NullableTarget, bool ConstElement, bool ConstTarget>
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
            return _process<NullableElement, NullableTarget, ConstElement, ConstTarget>(                          \
                    *down_cast<const ElementType*>(elements_ptr), array_offsets, *targets_ptr, null_map_elements, \
                    null_map_targets);                                                                            \
        }                                                                                                         \
    } while (0)

        HANDLE_ELEMENT_TYPE(ArrayColumn);
        HANDLE_ELEMENT_TYPE(JsonColumn);
        HANDLE_ELEMENT_TYPE(MapColumn);
        HANDLE_ELEMENT_TYPE(StructColumn);
#undef HANDLE_ELEMENT_TYPE

        return Status::NotSupported("unsupported operation for type: " + array_elements.get_name());
    }

    // array is non-nullable.
    template <bool ConstArray>
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
            return _array_contains<true, true, ConstArray, false>(*elements, offsets, *targets);
        } else if (nullable_element) {
            return const_target ? _array_contains<true, false, ConstArray, true>(*elements, offsets, *targets)
                                : _array_contains<true, false, ConstArray, false>(*elements, offsets, *targets);
        } else if (nullable_target) {
            return _array_contains<false, true, ConstArray, false>(*elements, offsets, *targets);
        } else {
            return const_target ? _array_contains<false, false, ConstArray, true>(*elements, offsets, *targets)
                                : _array_contains<false, false, ConstArray, false>(*elements, offsets, *targets);
        }
    }

    static StatusOr<ColumnPtr> _array_contains_generic(const Column& array, const Column& target) {
        bool is_const_array = array.is_constant();
        bool is_const_target = target.is_constant();
        const Column* data_column = &array;
        if (is_const_array) {
            data_column = down_cast<const ConstColumn*>(&array)->data_column().get();
        }

        const NullableColumn* nullable_column = nullptr;
        const ArrayColumn* array_column = nullptr;
        if (data_column->is_nullable()) {
            nullable_column = down_cast<const NullableColumn*>(data_column);
            array_column = down_cast<const ArrayColumn*>(nullable_column->data_column().get());
        } else {
            array_column = down_cast<const ArrayColumn*>(data_column);
        }

        ASSIGN_OR_RETURN(ColumnPtr result, is_const_array ? _array_contains_non_nullable<true>(*array_column, target)
                                                          : _array_contains_non_nullable<false>(*array_column, target));

        if (data_column->is_nullable()) {
            DCHECK_EQ(nullable_column->size(), result->size());
            if (nullable_column->has_null()) {
                result = NullableColumn::create(std::move(result), nullable_column->null_column()->clone());
            }
        }

        bool return_const = is_const_array && is_const_target;
        return return_const ? ConstColumn::create(std::move(result), target.size()) : result;
    }
};

template <bool Any, bool ContainsSeq>
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

    template <bool NullableElement, bool NullableTarget, typename ElementColumn>
    static uint8 __process_seq(const ElementColumn& elements, uint32 element_start, uint32 element_end,
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
        if (element_end - element_start < target_end - target_start) {
            return false;
        }
        if (target_end == target_start) {
            return true;
        }
        if (element_end == element_start) {
            return false;
        }
        bool found = false;
        size_t i = target_start;
        size_t j = element_start;
        while (j < element_end) {
            if (element_end - j < (target_end - target_start)) {
                return false;
            }
            int k = j;
            i = target_start;
            while (i < target_end) {
                bool null_target = false;
                if constexpr (NullableTarget) {
                    null_target = is_null(null_map_targets, i);
                }
                bool null_element = false;
                if constexpr (NullableElement) {
                    null_element = is_null(null_map_elements, k);
                }
                if (null_target && null_element) {
                    found = true;
                } else if (null_target || null_element) {
                    found = false;
                } else {
                    if constexpr (std::is_same_v<ArrayColumn, ElementColumn> ||
                                  std::is_same_v<MapColumn, ElementColumn> ||
                                  std::is_same_v<StructColumn, ElementColumn> ||
                                  std::is_same_v<JsonColumn, ElementColumn>) {
                        found = (elements.equals(k, targets, i) == 1);
                    } else {
                        auto elements_ptr = (const ValueType*)(elements.raw_data());
                        auto targets_ptr = (const ValueType*)(targets.raw_data());
                        found = (elements_ptr[k] == targets_ptr[i]);
                    }
                }
                if (found) {
                    i++;
                    k++;
                } else {
                    break;
                }
            }
            if (i == target_end) {
                return true;
            }
            j++;
        }
        return false;
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
            if constexpr (ContainsSeq) {
                DCHECK_EQ(num_array, num_target);
                found = __process_seq<NullableElement, NullableTarget, ElementColumn>(
                        elements, element_offsets_ptr[i], element_offsets_ptr[i + 1], targets, target_offsets_ptr[i],
                        target_offsets_ptr[i + 1], null_map_elements, null_map_targets);
            } else {
                if constexpr (ConstTarget) {
                    DCHECK_EQ(num_target, 1);
                    found = __process<NullableElement, NullableTarget, ElementColumn>(
                            elements, element_offsets_ptr[i], element_offsets_ptr[i + 1], targets,
                            target_offsets_ptr[0], target_offsets_ptr[1], null_map_elements, null_map_targets);
                } else {
                    DCHECK_EQ(num_array, num_target);
                    found = __process<NullableElement, NullableTarget, ElementColumn>(
                            elements, element_offsets_ptr[i], element_offsets_ptr[i + 1], targets,
                            target_offsets_ptr[i], target_offsets_ptr[i + 1], null_map_elements, null_map_targets);
                }
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

StatusOr<ColumnPtr> ArrayFunctions::array_contains_generic([[maybe_unused]] FunctionContext* context,
                                                           const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL({columns[0]});
    const ColumnPtr& arg0 = columns[0];
    const ColumnPtr& arg1 = columns[1]; // element

    return ArrayContainsImpl<false, UInt8Column>::evaluate(*arg0, *arg1);
}

StatusOr<ColumnPtr> ArrayFunctions::array_position_generic([[maybe_unused]] FunctionContext* context,
                                                           const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL({columns[0]});
    const ColumnPtr& arg0 = columns[0];
    const ColumnPtr& arg1 = columns[1]; // element

    return ArrayContainsImpl<true, Int32Column>::evaluate(*arg0, *arg1);
}

StatusOr<ColumnPtr> ArrayFunctions::array_contains_any([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = ColumnHelper::unpack_and_duplicate_const_column(columns[1]->size(), columns[1]); // element

    return ArrayHasImpl<true, false>::evaluate(*arg0, *arg1);
}

StatusOr<ColumnPtr> ArrayFunctions::array_contains_all([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = ColumnHelper::unpack_and_duplicate_const_column(columns[1]->size(), columns[1]); // element

    return ArrayHasImpl<false, false>::evaluate(*arg0, *arg1);
}

StatusOr<ColumnPtr> ArrayFunctions::array_contains_seq([[maybe_unused]] FunctionContext* context,
                                                       const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = ColumnHelper::unpack_and_duplicate_const_column(columns[1]->size(), columns[1]); // element

    return ArrayHasImpl<false, true>::evaluate(*arg0, *arg1);
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
            auto nullable_column = down_cast<const NullableColumn*>(column.get());
            if (nulls == nullptr) {
                nulls = NullColumn::static_pointer_cast(nullable_column->null_column()->clone());
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
            const auto nullable_column = down_cast<const NullableColumn*>(column.get());
            array_columns.emplace_back(ArrayColumn::static_pointer_cast(nullable_column->data_column()));
        } else if (column->is_constant()) {
            // NOTE: I'm not sure if there will be const array, just to be safe
            array_columns.emplace_back(ArrayColumn::static_pointer_cast(
                    ColumnHelper::unpack_and_duplicate_const_column(num_rows, column)));
        } else {
            array_columns.emplace_back(ArrayColumn::static_pointer_cast(column));
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
void _array_slice_item(const ArrayColumn* column, size_t index, ArrayColumn* dest_column, int64_t offset,
                       int64_t length) {
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

    const ArrayColumn* array_column = nullptr;
    if (columns[0]->is_nullable()) {
        is_nullable = true;
        has_null = (columns[0]->has_null() || has_null);

        const auto* src_nullable_column = down_cast<const NullableColumn*>(columns[0].get());
        array_column = down_cast<const ArrayColumn*>(src_nullable_column->data_column().get());
        null_result = NullColumn::create(*src_nullable_column->null_column());
    } else {
        array_column = down_cast<const ArrayColumn*>(src_column.get());
    }

    const Int64Column* offset_column = nullptr;
    if (columns[1]->is_nullable()) {
        is_nullable = true;
        has_null = (columns[1]->has_null() || has_null);

        const auto* src_nullable_column = down_cast<const NullableColumn*>(columns[1].get());
        offset_column = down_cast<const Int64Column*>(src_nullable_column->data_column().get());
        if (null_result) {
            null_result = FunctionHelper::union_null_column(null_result, src_nullable_column->null_column());
        } else {
            null_result = NullColumn::create(*src_nullable_column->null_column());
        }
    } else {
        offset_column = down_cast<const Int64Column*>(
                ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[1]).get());
    }

    const Int64Column* length_column = nullptr;
    // length_column is provided.
    if (columns.size() > 2) {
        if (columns[2]->is_nullable()) {
            is_nullable = true;
            has_null = (columns[2]->has_null() || has_null);

            const auto* src_nullable_column = down_cast<const NullableColumn*>(columns[2].get());
            length_column = down_cast<const Int64Column*>(src_nullable_column->data_column().get());
            if (null_result) {
                null_result = FunctionHelper::union_null_column(null_result, src_nullable_column->null_column());
            } else {
                null_result = NullColumn::create(*src_nullable_column->null_column());
            }
        } else {
            length_column = down_cast<const Int64Column*>(
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
            return NullableColumn::create(std::move(dest_column), std::move(null_result));
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

    // TODO: Maybe consume large memory, need optimized later.
    std::vector<uint32_t> hash(elements->size(), 0);
    elements->fnv_hash(hash.data(), 0, elements->size());

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

    return NullableColumn::create(ArrayColumn::create(std::move(result_elements), std::move(result_offsets)),
                                  std::move(array_null));
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
                                  std::move(array_null));
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

    NullColumnPtr nulls = NullColumn::create(rows, 0);
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

    return NullableColumn::create(ArrayColumn::create(base_elements, std::move(result_offsets)), std::move(nulls));
}

static Status sort_multi_array_column(FunctionContext* ctx, const Column* src_column, const NullColumn* src_null_column,
                                      const std::vector<const Column*>& key_columns, Column* dest_column) {
    const auto* src_elements_column = down_cast<const ArrayColumn*>(src_column)->elements_column().get();
    const auto* src_offsets_column = &down_cast<const ArrayColumn*>(src_column)->offsets();

    auto* dest_elements_column = down_cast<ArrayColumn*>(dest_column)->elements_column().get();
    auto* dest_offsets_column = down_cast<ArrayColumn*>(dest_column)->offsets_column().get();

    const auto num_src_element_rows = src_elements_column->size();
    const size_t num_rows = src_column->size();
    const size_t num_key_columns = key_columns.size();

    dest_offsets_column->get_data() = src_offsets_column->get_data();

    // Unpack each key array column.
    std::vector<const Column*> elements_per_key_col(num_key_columns);
    std::vector<std::span<const uint32_t>> offsets_per_key_col(num_key_columns);
    std::vector<const uint8_t*> nulls_per_key_col(num_key_columns, nullptr);
    for (size_t i = 0; i < num_key_columns; ++i) {
        const Column* key_column = key_columns[i];
        if (key_column->is_nullable()) {
            const auto* key_nullable_column = down_cast<const NullableColumn*>(key_column);
            nulls_per_key_col[i] = key_nullable_column->immutable_null_column_data().data();
            key_column = key_nullable_column->data_column().get();
        }

        const auto* key_array_column = down_cast<const ArrayColumn*>(key_column);
        // elements_per_key_col[i] = const_cast<Column*>(key_array_column->elements_column().get());
        elements_per_key_col[i] = key_array_column->elements_column().get();
        offsets_per_key_col[i] = key_array_column->offsets().get_data();
    }

    // Check if the number of elements in each array column of each row is exactly the same.
    for (size_t row_i = 0; row_i < num_rows; ++row_i) {
        if (src_null_column != nullptr && src_null_column->get_data()[row_i]) {
            continue;
        }

        const auto cur_num_src_elements =
                src_offsets_column->get_data()[row_i + 1] - src_offsets_column->get_data()[row_i];
        for (size_t key_col_i = 0; key_col_i < num_key_columns; ++key_col_i) {
            if (nulls_per_key_col[key_col_i] && nulls_per_key_col[key_col_i][row_i]) {
                continue;
            }

            const auto cur_num_key_elements =
                    offsets_per_key_col[key_col_i][row_i + 1] - offsets_per_key_col[key_col_i][row_i];
            if (cur_num_src_elements != cur_num_key_elements) {
                return Status::InvalidArgument("Input arrays' size are not equal in array_sortby.");
            }
        }
    }

    const SortDescs sort_desc = SortDescs::asc_null_first(num_key_columns);
    const std::atomic<bool>& cancel = ctx->state()->cancelled_ref();
    SmallPermutation permutation;
    RETURN_IF_ERROR(sort_and_tie_columns(cancel, elements_per_key_col, sort_desc, permutation,
                                         src_offsets_column->get_data(), offsets_per_key_col));

    std::vector<uint32_t> key_sort_index;
    raw::stl_vector_resize_uninitialized(&key_sort_index, num_src_element_rows);
    for (int i = 0; i < num_src_element_rows; i++) {
        key_sort_index[i] = permutation[i].index_in_chunk;
    }
    dest_elements_column->append_selective(*src_elements_column, key_sort_index);

    return Status::OK();
}

StatusOr<ColumnPtr> ArrayFunctions::array_sortby_multi(FunctionContext* ctx, const Columns& columns) {
    DCHECK_GE(columns.size(), 1);

    if (columns[0]->only_null()) {
        return columns[0];
    }

    const size_t chunk_size = columns[0]->size();
    auto* src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]).get();
    ColumnPtr dest_column = src_column->clone_empty();

    std::vector<const Column*> key_columns;
    key_columns.reserve(columns.size() - 1);
    for (size_t i = 1; i < columns.size(); ++i) {
        if (!columns[i]->only_null()) {
            const auto* key_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[i]).get();
            key_columns.emplace_back(key_column);
        }
    }

    if (!src_column->is_nullable()) {
        RETURN_IF_ERROR(sort_multi_array_column(ctx, src_column, nullptr, key_columns, dest_column.get()));
    } else {
        const auto* src_nullable_column = down_cast<const NullableColumn*>(src_column);
        const auto* src_data_column = src_nullable_column->data_column().get();
        const auto* src_null_column = src_nullable_column->null_column().get();

        auto* dest_nullable_column = down_cast<NullableColumn*>(dest_column.get());
        auto* dest_data_column = dest_nullable_column->mutable_data_column();
        auto* dest_null_column = dest_nullable_column->mutable_null_column();

        if (src_nullable_column->has_null()) {
            dest_null_column->get_data().assign(src_null_column->get_data().begin(), src_null_column->get_data().end());
        } else {
            dest_null_column->get_data().resize(chunk_size, 0);
            src_null_column = nullptr;
        }
        dest_nullable_column->set_has_null(src_nullable_column->has_null());

        RETURN_IF_ERROR(sort_multi_array_column(ctx, src_data_column, src_null_column, key_columns, dest_data_column));
    }

    return std::move(dest_column);
}

StatusOr<ColumnPtr> ArrayFunctions::repeat(FunctionContext* ctx, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    DCHECK(columns.size() == 2);

    const ColumnPtr& src_column = columns[0];
    size_t num_rows = src_column->size();

    const ColumnPtr& repeat_count_column = columns[1];
    ColumnViewer<TYPE_INT> repeat_count_viewer(repeat_count_column);

    ColumnPtr dest_column_elements = nullptr;
    // Because the _elements of ArrayColumn must be nullable, but a non-null ConstColumn cannot be converted to nullable;
    // therefore, the _data of ConstColumn is extracted as dest_column_elements.
    if (src_column->is_constant() && !src_column->is_nullable()) {
        const ConstColumn* const_src_column = down_cast<const ConstColumn*>(src_column.get());
        dest_column_elements = const_src_column->data_column()->clone_empty();
    } else {
        dest_column_elements = src_column->clone_empty();
    }
    auto dest_offsets = UInt32Column::create(1);
    size_t total_repeated_rows = 0;
    for (int cur_row = 0; cur_row < num_rows; cur_row++) {
        if (repeat_count_viewer.is_null(cur_row)) {
            dest_offsets->append(total_repeated_rows);
        } else {
            Datum source_value = src_column->get(cur_row);
            auto repeat_count = repeat_count_viewer.value(cur_row);
            if (repeat_count > 0) {
                for (int repeat_index = 0; repeat_index < repeat_count; repeat_index++) {
                    TRY_CATCH_BAD_ALLOC(dest_column_elements->append_datum(source_value));
                }
                total_repeated_rows = total_repeated_rows + repeat_count;
                dest_offsets->append(total_repeated_rows);
            } else {
                dest_offsets->append(total_repeated_rows);
            }
        }
    }

    ColumnPtr dest_column = nullptr;
    if (dest_column_elements->is_nullable()) {
        dest_column = ArrayColumn::create(std::move(dest_column_elements), std::move(dest_offsets));
    } else {
        auto nullable_dest_column_elements = NullableColumn::create(
                dest_column_elements, NullColumn::create(dest_column_elements->size(), DATUM_NOT_NULL));
        dest_column = ArrayColumn::create(std::move(nullable_dest_column_elements), std::move(dest_offsets));
    }

    NullColumnPtr null_result = nullptr;
    if (repeat_count_column->is_nullable()) {
        const auto* nullable_repeat_count_column = down_cast<const NullableColumn*>(repeat_count_column.get());
        null_result = NullColumn::create(*nullable_repeat_count_column->null_column());
    }

    if (null_result) {
        return NullableColumn::create(std::move(dest_column), std::move(null_result));
    } else {
        return dest_column;
    }
}

StatusOr<ColumnPtr> ArrayFunctions::array_flatten(FunctionContext* ctx, const Columns& columns) {
    DCHECK_EQ(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    size_t chunk_size = columns[0]->size();

    // Helper function to init result array elements and offsets
    auto build_result_array_elements_and_offsets = [](const ColumnPtr& elements_column,
                                                      size_t offsets_size) -> std::pair<ColumnPtr, UInt32Column::Ptr> {
        DCHECK(elements_column->is_array());
        auto [array_null, elements, offsets] = unpack_array_column(elements_column);
        auto result_elements = elements->clone_empty();
        auto result_offsets = UInt32Column::create();
        result_offsets->reserve(offsets_size);
        result_offsets->append(0);
        return std::make_pair(std::move(result_elements), std::move(result_offsets));
    };

    // Helper function to flatten a single array item
    auto flatten_array_item = [](const Datum& v, ColumnPtr& result_elements, auto& result_offsets) {
        if (!v.is_null()) {
            const auto& items = v.get<DatumArray>();
            for (const auto& item : items) {
                if (!item.is_null()) {
                    const auto& sub_items = item.get<DatumArray>();
                    for (const auto& sub_item : sub_items) {
                        result_elements->append_datum(sub_item);
                    }
                }
            }
        }
        result_offsets->append(result_elements->size());
    };

    // Special handle const column
    if (columns[0]->is_constant()) {
        const auto* const_column = down_cast<const ConstColumn*>(columns[0].get());
        const ArrayColumn* const_array = down_cast<const ArrayColumn*>(const_column->data_column().get());

        auto [result_elements, result_offsets] =
                build_result_array_elements_and_offsets(const_array->elements_column(), 1);
        Datum v = const_array->get(0);
        flatten_array_item(v, result_elements, result_offsets);
        return ConstColumn::create(ArrayColumn::create(result_elements, result_offsets), chunk_size);
    }

    const NullableColumn* src_nullable_column = nullptr;
    const ArrayColumn* array_column = nullptr;
    if (columns[0]->is_nullable()) {
        src_nullable_column = down_cast<const NullableColumn*>(columns[0].get());
        array_column = down_cast<const ArrayColumn*>(src_nullable_column->data_column().get());
    } else {
        array_column = down_cast<const ArrayColumn*>(columns[0].get());
    }

    auto [result_elements, result_offsets] =
            build_result_array_elements_and_offsets(array_column->elements_column(), array_column->offsets().size());
    for (size_t i = 0; i < chunk_size; i++) {
        Datum v = array_column->get(i);
        flatten_array_item(v, result_elements, result_offsets);
    }

    auto result = ArrayColumn::create(result_elements, result_offsets);
    if (src_nullable_column != nullptr) {
        return NullableColumn::create(result, src_nullable_column->null_column());
    }
    return result;
}
} // namespace starrocks
