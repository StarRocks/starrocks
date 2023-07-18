// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/array_functions.h"

#include "column/array_column.h"
#include "column/column_hash.h"
#include "column/type_traits.h"
#include "util/raw_container.h"

namespace starrocks::vectorized {

ColumnPtr ArrayFunctions::array_length([[maybe_unused]] FunctionContext* context, const Columns& columns) {
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

ColumnPtr ArrayFunctions::array_ndims([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return nullptr;
}

template <bool OnlyNullData, bool ConstData>
static ColumnPtr do_array_append(const Column& elements, const UInt32Column& offsets, const Column& data) {
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
ColumnPtr ArrayFunctions::array_append([[maybe_unused]] FunctionContext* context, const Columns& columns) {
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

    ColumnPtr result;
    if (arg1->only_null()) {
        result = do_array_append<true, true>(*element, *offsets, *arg1);
    } else if (const_data) {
        result = do_array_append<false, true>(*element, *offsets, *arg1);
    } else {
        result = do_array_append<false, false>(*element, *offsets, *arg1);
    }

    if (nullable_array != nullptr) {
        return NullableColumn::create(std::move(result), nullable_array->null_column());
    }
    return result;
}

class ArrayRemoveImpl {
public:
    static ColumnPtr evaluate(const ColumnPtr array, const ColumnPtr element) {
        return _array_remove_generic(array, element);
    }

private:
    template <bool NullableElement, bool NullableTarget, bool ConstTarget, typename ElementColumn,
              typename TargetColumn>
    static ColumnPtr _process(const ElementColumn& elements, const UInt32Column& offsets, const TargetColumn& targets,
                              const NullColumn::Container* null_map_elements,
                              const NullColumn::Container* null_map_targets) {
        const size_t num_array = offsets.size() - 1;

        auto result_array = ArrayColumn::create(NullableColumn::create(elements.clone_empty(), NullColumn::create()),
                                                UInt32Column::create());
        UInt32Column::Container& result_offsets = result_array->offsets_column()->get_data();
        ColumnPtr& result_elements = result_array->elements_column();

        result_offsets.reserve(num_array);

        using ValueType = std::conditional_t<std::is_same_v<ArrayColumn, ElementColumn>, uint8_t,
                                             typename ElementColumn::ValueType>;

        auto offsets_ptr = offsets.get_data().data();
        [[maybe_unused]] auto elements_ptr = (const ValueType*)(elements.raw_data());
        auto targets_ptr = (const ValueType*)(targets.raw_data());
        auto& first_target = *targets_ptr;

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
                if constexpr (std::is_same_v<ArrayColumn, ElementColumn>) {
                    found = (elements.compare_at(offset + j, i, targets, -1) == 0);
                } else if constexpr (ConstTarget) {
                    found = (elements_ptr[offset + j] == first_target);
                } else {
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
    static ColumnPtr _array_remove(const Column& array_elements, const UInt32Column& array_offsets,
                                   const Column& argument) {
        const Column* elements_ptr = &array_elements;
        const Column* targets_ptr = &argument;

        const NullColumn::Container* null_map_elements = nullptr;
        const NullColumn::Container* null_map_targets = nullptr;

        if constexpr (NullableElement) {
            const NullableColumn& nullable = down_cast<const NullableColumn&>(array_elements);
            elements_ptr = nullable.data_column().get();
            null_map_elements = &(nullable.null_column()->get_data());
        }

        if constexpr (NullableTarget) {
            const NullableColumn& nullable = down_cast<const NullableColumn&>(argument);
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

        LOG(ERROR) << "unhandled column type: " << typeid(array_elements).name();
        DCHECK(false) << "unhandled column type: " << typeid(array_elements).name();
        return ColumnHelper::create_const_null_column(array_elements.size());
    }

    // array is non-nullable.
    static ColumnPtr _array_remove_non_nullable(const ArrayColumn& array, const Column& arg) {
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

    static ColumnPtr _array_remove_generic(const ColumnPtr& array, const ColumnPtr& target) {
        if (array->only_null()) {
            return array;
        }
        if (auto nullable = dynamic_cast<const NullableColumn*>(array.get()); nullable != nullptr) {
            auto array_col = down_cast<const ArrayColumn*>(nullable->data_column().get());
            auto result = _array_remove_non_nullable(*array_col, *target);
            DCHECK_EQ(nullable->size(), result->size());
            return NullableColumn::create(std::move(result), nullable->null_column());
        }

        return _array_remove_non_nullable(down_cast<ArrayColumn&>(*array), *target);
    }
};

template <PrimitiveType TYPE>
struct ArrayCumSumImpl {
public:
    static ColumnPtr evaluate(const ColumnPtr& col) {
        if (col->is_constant()) {
            ConstColumn* input = down_cast<ConstColumn*>(col.get());
            auto arr_col_h = input->data_column()->clone();
            ArrayColumn* arr_col = down_cast<ArrayColumn*>(arr_col_h.get());
            call_cum_sum(arr_col, nullptr);
            return ConstColumn::create(std::move(arr_col_h));
        } else if (col->is_nullable()) {
            auto res = col->clone();
            NullableColumn* input = down_cast<NullableColumn*>(res.get());
            NullColumn* null_column = input->mutable_null_column();
            ArrayColumn* arr_col = down_cast<ArrayColumn*>(input->data_column().get());
            call_cum_sum(arr_col, null_column);
            return res;
        } else {
            auto res = col->clone();
            ArrayColumn* arr_col = down_cast<ArrayColumn*>(res.get());
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

#define DEFINE_ARRAY_CUMSUM_FN(NAME, TYPE)                                                    \
    ColumnPtr ArrayFunctions::array_cum_sum_##NAME([[maybe_unused]] FunctionContext* context, \
                                                   const Columns& columns) {                  \
        DCHECK_EQ(columns.size(), 1);                                                         \
        RETURN_IF_COLUMNS_ONLY_NULL(columns);                                                 \
        const ColumnPtr& arg0 = columns[0];                                                   \
                                                                                              \
        return ArrayCumSumImpl<TYPE>::evaluate(arg0);                                         \
    }

DEFINE_ARRAY_CUMSUM_FN(bigint, TYPE_BIGINT)
DEFINE_ARRAY_CUMSUM_FN(double, TYPE_DOUBLE)

#undef DEFINE_ARRAY_CUMSUM_FN

ColumnPtr ArrayFunctions::array_remove([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL({columns[0]});
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = columns[1];

    return ArrayRemoveImpl::evaluate(arg0, arg1);
}

// If PositionEnabled=true and ReturnType=Int32, it is function array_position and it will return index of elemt if the array contain it or 0 if not contain.
// If PositionEnabled=false and ReturnType=UInt8, it is function array_contains and it will return 1 if contain or 0 if not contain.
template <bool PositionEnabled, typename ReturnType>
class ArrayContainsImpl {
public:
    static ColumnPtr evaluate(const Column& array, const Column& element) {
        return _array_contains_generic(array, element);
    }

private:
    template <bool NullableElement, bool NullableTarget, bool ConstTarget, typename ElementColumn,
              typename TargetColumn>
    static ColumnPtr _process(const ElementColumn& elements, const UInt32Column& offsets, const TargetColumn& targets,
                              const NullColumn::Container* null_map_elements,
                              const NullColumn::Container* null_map_targets) {
        const size_t num_array = offsets.size() - 1;
        auto result = ReturnType::create();
        result->resize(num_array);

        auto* result_ptr = result->get_data().data();

        using ValueType = std::conditional_t<std::is_same_v<ArrayColumn, ElementColumn>, uint8_t,
                                             typename ElementColumn::ValueType>;

        auto offsets_ptr = offsets.get_data().data();
        [[maybe_unused]] auto elements_ptr = (const ValueType*)(elements.raw_data());
        auto targets_ptr = (const ValueType*)(targets.raw_data());
        auto& first_target = *targets_ptr;

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
                if constexpr (std::is_same_v<ArrayColumn, ElementColumn>) {
                    found = (elements.compare_at(offset + j, i, targets, -1) == 0);
                } else if constexpr (ConstTarget) {
                    found = (elements_ptr[offset + j] == first_target);
                } else {
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
    static ColumnPtr _array_contains(const Column& array_elements, const UInt32Column& array_offsets,
                                     const Column& argument) {
        const Column* elements_ptr = &array_elements;
        const Column* targets_ptr = &argument;

        const NullColumn::Container* null_map_elements = nullptr;
        const NullColumn::Container* null_map_targets = nullptr;

        if constexpr (NullableElement) {
            const NullableColumn& nullable = down_cast<const NullableColumn&>(array_elements);
            elements_ptr = nullable.data_column().get();
            null_map_elements = &(nullable.null_column()->get_data());
        }

        if constexpr (NullableTarget) {
            const NullableColumn& nullable = down_cast<const NullableColumn&>(argument);
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

        // TODO(zhuming): demangle class name
        LOG(ERROR) << "unhandled column type: " << typeid(array_elements).name();
        DCHECK(false) << "unhandled column type: " << typeid(array_elements).name();
        return ColumnHelper::create_const_null_column(array_elements.size());
    }

    // array is non-nullable.
    static ColumnPtr _array_contains_non_nullable(const ArrayColumn& array, const Column& arg) {
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

    static ColumnPtr _array_contains_generic(const Column& array, const Column& target) {
        // array_contains(NULL, xxx) -> NULL
        if (array.only_null()) {
            auto result = NullableColumn::create(ReturnType::create(), NullColumn::create());
            result->append_nulls(array.size());
            return result;
        }
        if (auto nullable = dynamic_cast<const NullableColumn*>(&array); nullable != nullptr) {
            auto array_col = down_cast<const ArrayColumn*>(nullable->data_column().get());
            auto result = _array_contains_non_nullable(*array_col, target);
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
    static ColumnPtr evaluate(const Column& array, const Column& element) { return _array_has_generic(array, element); }

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
                if constexpr (std::is_same_v<ArrayColumn, ElementColumn>) {
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
    static ColumnPtr _process(const ElementColumn& elements, const UInt32Column& element_offsets,
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
    static ColumnPtr _array_has(const Column& array_elements, const UInt32Column& array_offsets,
                                const Column& array_targets, const UInt32Column& target_offsets) {
        const Column* elements_ptr = &array_elements;
        const Column* targets_ptr = &array_targets;

        const NullColumn::Container* null_map_elements = nullptr;
        const NullColumn::Container* null_map_targets = nullptr;

        if constexpr (NullableElement) {
            const NullableColumn& nullable = down_cast<const NullableColumn&>(array_elements);
            elements_ptr = nullable.data_column().get();
            null_map_elements = &(nullable.null_column()->get_data());
        }

        if constexpr (NullableTarget) {
            const NullableColumn& nullable = down_cast<const NullableColumn&>(array_targets);
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

        LOG(ERROR) << "unhandled column type: " << typeid(array_elements).name();
        DCHECK(false) << "unhandled column type: " << typeid(array_elements).name();
        return ColumnHelper::create_const_null_column(array_elements.size());
    }

    // array is non-nullable.
    static ColumnPtr _array_has_non_nullable(const ArrayColumn& array, const ArrayColumn& arg) {
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

    static ColumnPtr _array_has_generic(const Column& array, const Column& target) {
        // has_any(NULL, xxx) | has_any(xxx, NULL) -> NULL
        if (array.only_null() || target.only_null()) {
            auto result = NullableColumn::create(Int8Column::create(), NullColumn::create());
            result->append_nulls(array.size());
            return result;
        }
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

<<<<<<< HEAD:be/src/exprs/vectorized/array_functions.cpp
        auto result = _array_has_non_nullable(*array_col, *target_col);
        DCHECK_EQ(array_nullable->size(), result->size());
=======
        ASSIGN_OR_RETURN(auto result, _array_has_non_nullable(*array_col, *target_col));
        DCHECK_EQ(array_col->size(), result->size());
>>>>>>> 7c982ad29 ([BugFix] fix array_has_generic DCHECK error (#27396)):be/src/exprs/array_functions.cpp
        return NullableColumn::create(std::move(result), merge_nullcolum(array_nullable, target_nullable));
    }
};

ColumnPtr ArrayFunctions::array_contains([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL({columns[0]});
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = columns[1];

    return ArrayContainsImpl<false, UInt8Column>::evaluate(*arg0, *arg1);
}

ColumnPtr ArrayFunctions::array_position([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL({columns[0]});
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = columns[1];

    return ArrayContainsImpl<true, Int32Column>::evaluate(*arg0, *arg1);
}

ColumnPtr ArrayFunctions::array_contains_any([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = ColumnHelper::unpack_and_duplicate_const_column(columns[1]->size(), columns[1]); // element

    return ArrayHasImpl<true>::evaluate(*arg0, *arg1);
}

ColumnPtr ArrayFunctions::array_contains_all([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const ColumnPtr& arg0 = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]); // array
    const ColumnPtr& arg1 = ColumnHelper::unpack_and_duplicate_const_column(columns[1]->size(), columns[1]); // element
    return ArrayHasImpl<false>::evaluate(*arg0, *arg1);
}

class ArrayArithmeticImpl {
public:
    using ArithmeticType = typename ArrayFunctions::ArithmeticType;

    template <ArithmeticType type, PrimitiveType value_type, PrimitiveType sum_result_type,
              PrimitiveType avg_result_type, bool has_null, typename ElementColumn>
    static ColumnPtr _sum_and_avg(const ElementColumn& elements, const UInt32Column& offsets,
                                  const NullColumn::Container* null_elements, std::vector<uint8_t>* null_ptr) {
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
                if constexpr (pt_is_datetime<value_type>) {
                    sum += value.to_unix_second();
                } else if constexpr (pt_is_date<value_type>) {
                    sum += value.julian();
                } else {
                    sum += value;
                }
            }

            if (has_data) {
                if constexpr (pt_is_decimalv2<value_type>) {
                    if constexpr (type == ArithmeticType::SUM) {
                        result_column->append(sum);
                    } else {
                        result_column->append(sum / DecimalV2Value(array_size, 0));
                    }
                } else if constexpr (pt_is_arithmetic<value_type> || pt_is_decimal<value_type>) {
                    if constexpr (type == ArithmeticType::SUM) {
                        result_column->append(sum);
                    } else {
                        result_column->append(sum / array_size);
                    }
                } else if constexpr (pt_is_datetime<value_type>) {
                    static_assert(type == ArithmeticType::AVG);
                    TimestampValue value;
                    value.from_unix_second(sum / array_size);
                    result_column->append(value);
                } else if constexpr (pt_is_date<value_type>) {
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

    template <bool is_min, ArithmeticType type, PrimitiveType value_type, bool has_null, typename ElementColumn>
    static ColumnPtr _min_and_max(const ElementColumn& elements, const UInt32Column& offsets,
                                  const NullColumn::Container* null_elements, std::vector<uint8_t>* null_ptr) {
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
                if constexpr (!pt_is_binary<value_type>) {
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

                if constexpr (!pt_is_binary<value_type>) {
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

template <PrimitiveType column_type, bool has_null, ArrayFunctions::ArithmeticType type>
ColumnPtr ArrayFunctions::_array_process_not_nullable_types(const Column* elements, const UInt32Column& offsets,
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

template <PrimitiveType column_type, ArrayFunctions::ArithmeticType type>
ColumnPtr ArrayFunctions::_array_process_not_nullable(const Column* raw_array_column, std::vector<uint8_t>* null_ptr) {
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

template <PrimitiveType column_type, ArrayFunctions::ArithmeticType type>
ColumnPtr ArrayFunctions::array_arithmetic(const Columns& columns) {
    DCHECK_EQ(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const ColumnPtr& array_column = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]);
    const auto& raw_array_column = *array_column;

    if (auto nullable = dynamic_cast<const NullableColumn*>(&raw_array_column); nullable != nullptr) {
        auto array_col = down_cast<const ArrayColumn*>(nullable->data_column().get());
        auto null_column = NullColumn::create(*nullable->null_column());
        auto result = ArrayFunctions::template _array_process_not_nullable<column_type, type>(array_col,
                                                                                              &null_column->get_data());

        DCHECK_EQ(nullable->size(), result->size());
        return NullableColumn::create(std::move(result), null_column);
    } else {
        auto null_column = NullColumn::create();
        null_column->resize(raw_array_column.size());
        auto result = ArrayFunctions::template _array_process_not_nullable<column_type, type>(&raw_array_column,
                                                                                              &null_column->get_data());
        return NullableColumn::create(std::move(result), null_column);
    }
}

template <PrimitiveType type>
ColumnPtr ArrayFunctions::array_sum(const Columns& columns) {
    return ArrayFunctions::template array_arithmetic<type, ArithmeticType::SUM>(columns);
}

ColumnPtr ArrayFunctions::array_sum_boolean([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_BOOLEAN>(columns);
}

ColumnPtr ArrayFunctions::array_sum_tinyint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_TINYINT>(columns);
}

ColumnPtr ArrayFunctions::array_sum_smallint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_SMALLINT>(columns);
}

ColumnPtr ArrayFunctions::array_sum_int([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_INT>(columns);
}

ColumnPtr ArrayFunctions::array_sum_bigint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_BIGINT>(columns);
}

ColumnPtr ArrayFunctions::array_sum_largeint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_LARGEINT>(columns);
}

ColumnPtr ArrayFunctions::array_sum_float([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_FLOAT>(columns);
}

ColumnPtr ArrayFunctions::array_sum_double([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_DOUBLE>(columns);
}

ColumnPtr ArrayFunctions::array_sum_decimalv2([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_sum<TYPE_DECIMALV2>(columns);
}

template <PrimitiveType type>
ColumnPtr ArrayFunctions::array_avg(const Columns& columns) {
    return ArrayFunctions::template array_arithmetic<type, ArithmeticType::AVG>(columns);
}

ColumnPtr ArrayFunctions::array_avg_boolean([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_BOOLEAN>(columns);
}

ColumnPtr ArrayFunctions::array_avg_tinyint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_TINYINT>(columns);
}

ColumnPtr ArrayFunctions::array_avg_smallint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_SMALLINT>(columns);
}

ColumnPtr ArrayFunctions::array_avg_int([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_INT>(columns);
}

ColumnPtr ArrayFunctions::array_avg_bigint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_BIGINT>(columns);
}

ColumnPtr ArrayFunctions::array_avg_largeint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_LARGEINT>(columns);
}

ColumnPtr ArrayFunctions::array_avg_float([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_FLOAT>(columns);
}

ColumnPtr ArrayFunctions::array_avg_double([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_DOUBLE>(columns);
}

ColumnPtr ArrayFunctions::array_avg_decimalv2([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_DECIMALV2>(columns);
}

ColumnPtr ArrayFunctions::array_avg_date([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_DATE>(columns);
}

ColumnPtr ArrayFunctions::array_avg_datetime([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_avg<TYPE_DATETIME>(columns);
}

template <PrimitiveType type>
ColumnPtr ArrayFunctions::array_min(const Columns& columns) {
    return ArrayFunctions::template array_arithmetic<type, ArithmeticType::MIN>(columns);
}

ColumnPtr ArrayFunctions::array_min_boolean([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_BOOLEAN>(columns);
}

ColumnPtr ArrayFunctions::array_min_tinyint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_TINYINT>(columns);
}

ColumnPtr ArrayFunctions::array_min_smallint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_SMALLINT>(columns);
}

ColumnPtr ArrayFunctions::array_min_int([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_INT>(columns);
}

ColumnPtr ArrayFunctions::array_min_bigint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_BIGINT>(columns);
}

ColumnPtr ArrayFunctions::array_min_largeint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_LARGEINT>(columns);
}

ColumnPtr ArrayFunctions::array_min_float([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_FLOAT>(columns);
}

ColumnPtr ArrayFunctions::array_min_double([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_DOUBLE>(columns);
}

ColumnPtr ArrayFunctions::array_min_decimalv2([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_DECIMALV2>(columns);
}

ColumnPtr ArrayFunctions::array_min_date([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_DATE>(columns);
}

ColumnPtr ArrayFunctions::array_min_datetime([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_DATETIME>(columns);
}

ColumnPtr ArrayFunctions::array_min_varchar([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_min<TYPE_VARCHAR>(columns);
}

template <PrimitiveType type>
ColumnPtr ArrayFunctions::array_max(const Columns& columns) {
    return ArrayFunctions::template array_arithmetic<type, ArithmeticType::MAX>(columns);
}

ColumnPtr ArrayFunctions::array_max_boolean([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_BOOLEAN>(columns);
}

ColumnPtr ArrayFunctions::array_max_tinyint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_TINYINT>(columns);
}

ColumnPtr ArrayFunctions::array_max_smallint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_SMALLINT>(columns);
}

ColumnPtr ArrayFunctions::array_max_int([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_INT>(columns);
}

ColumnPtr ArrayFunctions::array_max_bigint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_BIGINT>(columns);
}

ColumnPtr ArrayFunctions::array_max_largeint([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_LARGEINT>(columns);
}

ColumnPtr ArrayFunctions::array_max_float([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_FLOAT>(columns);
}

ColumnPtr ArrayFunctions::array_max_double([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_DOUBLE>(columns);
}

ColumnPtr ArrayFunctions::array_max_decimalv2([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_DECIMALV2>(columns);
}

ColumnPtr ArrayFunctions::array_max_date([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_DATE>(columns);
}

ColumnPtr ArrayFunctions::array_max_datetime([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_DATETIME>(columns);
}

ColumnPtr ArrayFunctions::array_max_varchar([[maybe_unused]] FunctionContext* context, const Columns& columns) {
    return ArrayFunctions::template array_max<TYPE_VARCHAR>(columns);
}

} // namespace starrocks::vectorized
