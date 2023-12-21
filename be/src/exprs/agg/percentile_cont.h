// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstring>
#include <limits>
#include <type_traits>

#include "column/column_helper.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "util/orlp/pdqsort.h"

namespace starrocks::vectorized {

// AvgResultLT for final result
template <PrimitiveType PT, bool isCont, typename = guard::Guard>
inline constexpr PrimitiveType PercentileResultPT = PT;

template <PrimitiveType PT>
inline constexpr PrimitiveType PercentileResultPT<PT, true, ArithmeticPTGuard<PT>> = TYPE_DOUBLE;

template <PrimitiveType LT, typename = guard::Guard>
struct PercentileState {
    using CppType = RunTimeCppType<LT>;
    void update(CppType item) { items.emplace_back(item); }
    void update_batch(const std::vector<CppType>& vec) {
        size_t old_size = items.size();
        items.resize(old_size + vec.size());
        memcpy(items.data() + old_size, vec.data(), vec.size() * sizeof(CppType));
    }
    std::vector<CppType> items;
    std::vector<std::vector<CppType>> grid;
    double rate = 0.0;
};

<<<<<<< HEAD
template <PrimitiveType PT>
=======
template <LogicalType LT, typename CppType, bool reverse>
void kWayMergeSort(const std::vector<std::vector<CppType>>& grid, std::vector<CppType>& b, std::vector<int>& ls,
                   std::map<int, int>& mp, size_t goal, int k, CppType& junior_elm, CppType& senior_elm) {
    CppType minV = RunTimeTypeLimits<LT>::min_value();
    CppType maxV = RunTimeTypeLimits<LT>::max_value();

    b.resize(k + 1);
    ls.resize(k);
    for (int i = 0; i < k; ++i) {
        if constexpr (reverse) {
            mp[i] = grid[i].size() - 2;
        } else {
            mp[i] = 1;
        }
    }
    for (int i = 0; i < k; ++i) {
        b[i] = grid[i][mp[i]];
        if constexpr (reverse) {
            mp[i]--;
        } else {
            mp[i]++;
        }
    }
    b[k] = reverse ? maxV : minV;
    for (int i = 0; i < k; ++i) {
        ls[i] = k;
    }

    for (int i = k - 1; i >= 0; --i) {
        int q = i;
        int t = (q + k) / 2;
        while (t > 0) {
            if constexpr (reverse) {
                if (b[q] < b[ls[t]]) {
                    std::swap(q, ls[t]);
                }
            } else if (b[q] > b[ls[t]]) {
                std::swap(q, ls[t]);
            }
            t = t / 2;
        }
        ls[0] = q;
    }

    CppType tp = reverse ? minV : maxV;
    size_t cnt = 0;

    while (b[ls[0]] != tp) {
        int q = ls[0];
        if (UNLIKELY(cnt >= goal)) {
            if (cnt == goal) {
                if constexpr (reverse)
                    senior_elm = b[q];
                else
                    junior_elm = b[q];
            }
            if (cnt == goal + 1) {
                if constexpr (reverse)
                    junior_elm = b[q];
                else
                    senior_elm = b[q];
                break;
            }
        }
        cnt++;
        b[q] = grid[q][mp[q]];

        if constexpr (reverse) {
            mp[q]--;
        } else {
            mp[q]++;
        }
        int t = (q + k) / 2;
        while (t > 0) {
            if constexpr (reverse) {
                if (b[q] < b[ls[t]]) {
                    std::swap(q, ls[t]);
                }
            } else if (b[q] > b[ls[t]]) {
                std::swap(q, ls[t]);
            }
            t = t / 2;
        }
        ls[0] = q;
    }
}

template <LogicalType LT, typename CppType, typename ResultType>
ResultType calculateResult(CppType junior_elm, CppType senior_elm, double u, size_t index) {
    [[maybe_unused]] ResultType result;
    if constexpr (lt_is_datetime<LT>) {
        result.from_unix_second(junior_elm.to_unix_second() +
                                (u - (float)index) * (senior_elm.to_unix_second() - junior_elm.to_unix_second()));
    } else if constexpr (lt_is_date<LT>) {
        result._julian = junior_elm._julian + (u - (double)index) * (senior_elm._julian - junior_elm._julian);
    } else if constexpr (lt_is_arithmetic<LT>) {
        result = junior_elm + (u - (double)index) * (senior_elm - junior_elm);
    } else {
        // won't go there if percentile_cont is registered correctly
        throw std::runtime_error("Invalid PrimitiveTypes: " + type_to_string(LT) + " for percentile_cont function");
    }
    return result;
}

template <LogicalType LT, typename = guard::Guard>
>>>>>>> 50051cf9a9 ([Enhancement] percentile_cont performance enhance (#36120))
class PercentileContDiscAggregateFunction
        : public AggregateFunctionBatchHelper<PercentileState<PT>, PercentileContDiscAggregateFunction<PT>> {
public:
    using InputCppType = RunTimeCppType<PT>;
    using InputColumnType = RunTimeColumnType<PT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        this->data(state).update(column.get_data()[row_num]);

        if (ctx->get_num_args() == 2) {
            const auto* rate = down_cast<const ConstColumn*>(columns[1]);
            this->data(state).rate = rate->get(row_num).get_double();
            DCHECK(this->data(state).rate >= 0 && this->data(state).rate <= 1);
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        this->data(state).update_batch(column.get_data());

        if (ctx->get_num_args() == 2) {
            const auto* rate = down_cast<const ConstColumn*>(columns[1]);
            this->data(state).rate = rate->get(0).get_double();
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());

        const Slice slice = column->get(row_num).get_slice();
        double rate = *reinterpret_cast<double*>(slice.data);
        size_t items_size = *reinterpret_cast<size_t*>(slice.data + sizeof(double));
        auto data_ptr = slice.data + sizeof(double) + sizeof(size_t);
        std::vector<std::vector<InputCppType>>& grid = this->data(state).grid;

        std::vector<InputCppType> vec;
        vec.resize(items_size + 2);
        memcpy(vec.data() + 1, data_ptr, items_size * sizeof(InputCppType));
        vec[0] = RunTimeTypeLimits<LT>::min_value();
        vec[vec.size() - 1] = RunTimeTypeLimits<LT>::max_value();

        grid.emplace_back(std::move(vec));
        this->data(state).rate = rate;
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();
        size_t old_size = bytes.size();
        size_t items_size = this->data(state).items.size();
        size_t grid_items_size = 0;
        for (const auto& vec : this->data(state).grid) {
            grid_items_size += vec.size() - 2;
        }
        size_t total_items_size = items_size + grid_items_size;

        // should serialize: rate_size, vector_size, all vector element.
        size_t new_size = old_size + sizeof(double) + sizeof(size_t) + total_items_size * sizeof(InputCppType);
        bytes.resize(new_size);

        memcpy(bytes.data() + old_size, &(this->data(state).rate), sizeof(double));
        memcpy(bytes.data() + old_size + sizeof(double), &total_items_size, sizeof(size_t));
        memcpy(bytes.data() + old_size + sizeof(double) + sizeof(size_t), this->data(state).items.data(),
               items_size * sizeof(InputCppType));
<<<<<<< HEAD
        pdqsort(false, reinterpret_cast<InputCppType*>(bytes.data() + old_size + sizeof(double) + sizeof(size_t)),
=======
        size_t current_pos = old_size + sizeof(double) + sizeof(size_t) + items_size * sizeof(InputCppType);
        for (const auto& vec : this->data(state).grid) {
            memcpy(bytes.data() + current_pos, vec.data() + 1, (vec.size() - 2) * sizeof(InputCppType));
            current_pos += (vec.size() - 2) * sizeof(InputCppType);
        }

        pdqsort(reinterpret_cast<InputCppType*>(bytes.data() + old_size + sizeof(double) + sizeof(size_t)),
>>>>>>> 50051cf9a9 ([Enhancement] percentile_cont performance enhance (#36120))
                reinterpret_cast<InputCppType*>(bytes.data() + old_size + sizeof(double) + sizeof(size_t) +
                                                total_items_size * sizeof(InputCppType)));

        column->get_offset().emplace_back(new_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        if (chunk_size <= 0) {
            return;
        }
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = dst_column->get_bytes();
        double rate = ColumnHelper::get_const_value<TYPE_DOUBLE>(src[1]);
        InputColumnType src_column = *down_cast<const InputColumnType*>(src[0].get());
        InputCppType* src_data = src_column.get_data().data();
        for (auto i = 0; i < chunk_size; ++i) {
            size_t old_size = bytes.size();
            bytes.resize(old_size + sizeof(double) + sizeof(size_t) + sizeof(InputCppType));
            memcpy(bytes.data() + old_size, &rate, sizeof(double));
            *reinterpret_cast<size_t*>(bytes.data() + old_size + sizeof(double)) = 1UL;
            memcpy(bytes.data() + old_size + sizeof(double) + sizeof(size_t), &src_data[i], sizeof(InputCppType));
            dst_column->get_offset().push_back(bytes.size());
        }
    }
};

template <PrimitiveType PT>
class PercentileContAggregateFunction final : public PercentileContDiscAggregateFunction<PT> {
    using InputCppType = RunTimeCppType<PT>;
    using InputColumnType = RunTimeColumnType<PT>;
    static constexpr auto ResultPT = PercentileResultPT<PT, true>;
    using ResultType = RunTimeCppType<ResultPT>;
    using ResultColumnType = RunTimeColumnType<ResultPT>;

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
<<<<<<< HEAD
        using CppType = RunTimeCppType<PT>;
        std::vector<CppType> new_vector = std::move(this->data(state).items);
        pdqsort(false, new_vector.begin(), new_vector.end());
        const double& rate = this->data(state).rate;

        ResultColumnType* column = down_cast<ResultColumnType*>(to);
        DCHECK(!new_vector.empty());
        if (new_vector.size() == 1 || rate == 1) {
            column->append(new_vector.back());
=======
        const std::vector<std::vector<InputCppType>>& grid = this->data(state).grid;
        const double& rate = this->data(state).rate;

        // for group by
        if (grid.size() == 0) {
            ResultColumnType* column = down_cast<ResultColumnType*>(to);
            auto& items = const_cast<std::vector<InputCppType>&>(this->data(state).items);
            std::sort(items.begin(), items.end());

            if (items.size() == 0) {
                return;
            }
            if (items.size() == 1 || rate == 1) {
                column->append(items.back());
                return;
            }

            double u = (items.size() - 1) * rate;
            auto index = (size_t)u;

            ResultType result = calculateResult<LT, InputCppType, ResultType>(items[index], items[index + 1], u, index);
            column->append(result);
>>>>>>> 50051cf9a9 ([Enhancement] percentile_cont performance enhance (#36120))
            return;
        }

        std::vector<InputCppType> b;
        std::vector<int> ls;
        std::map<int, int> mp;

<<<<<<< HEAD
        [[maybe_unused]] ResultType result;
        if constexpr (pt_is_datetime<PT>) {
            result.from_unix_second(
                    new_vector[index].to_unix_second() +
                    (u - (float)index) * (new_vector[index + 1].to_unix_second() - new_vector[index].to_unix_second()));
        } else if constexpr (pt_is_date<PT>) {
            result._julian = new_vector[index]._julian +
                             (u - (double)index) * (new_vector[index + 1]._julian - new_vector[index]._julian);
        } else if constexpr (pt_is_arithmetic<PT>) {
            result = new_vector[index] + (u - (double)index) * (new_vector[index + 1] - new_vector[index]);
        } else {
            // won't go there if percentile_cont is registered correctly
            throw std::runtime_error("Invalid PrimitiveTypes for percentile_cont function");
=======
        size_t k = grid.size();
        size_t rowsNum = 0;
        for (int i = 0; i < k; i++) {
            rowsNum += grid[i].size() - 2;
        }
        if (rowsNum == 0) return;

        bool reverse = false;
        if (rate > 0.5 && rowsNum > 2) reverse = true;

        double u = ((double)rowsNum - 1) * rate;
        auto index = (size_t)u;
        size_t goal = reverse ? (size_t)ceil((double)rowsNum - 2 - u) : (size_t)u;
        if (rate == 1) {
            goal = 0;
>>>>>>> 50051cf9a9 ([Enhancement] percentile_cont performance enhance (#36120))
        }

        InputCppType junior_elm;
        InputCppType senior_elm;

        if (reverse) {
            kWayMergeSort<LT, InputCppType, true>(grid, b, ls, mp, goal, k, junior_elm, senior_elm);
        } else {
            kWayMergeSort<LT, InputCppType, false>(grid, b, ls, mp, goal, k, junior_elm, senior_elm);
        }

        ResultColumnType* column = down_cast<ResultColumnType*>(to);

        if (rate == 0) {
            column->append(junior_elm);
            return;
        } else if (rate == 1) {
            column->append(senior_elm);
            return;
        }

        ResultType result = calculateResult<LT, InputCppType, ResultType>(junior_elm, senior_elm, u, index);
        column->append(result);
    }

    std::string get_name() const override { return "percentile_cont"; }
};

template <PrimitiveType PT>
class PercentileDiscAggregateFunction final : public PercentileContDiscAggregateFunction<PT> {
    using InputCppType = RunTimeCppType<PT>;
    using InputColumnType = RunTimeColumnType<PT>;
    static constexpr auto ResultLT = PercentileResultPT<PT, false>;
    using ResultType = RunTimeCppType<ResultLT>;
    using ResultColumnType = RunTimeColumnType<ResultLT>;

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
<<<<<<< HEAD
        using CppType = RunTimeCppType<PT>;
        std::vector<CppType> new_vector = std::move(this->data(state).items);
        pdqsort(false, new_vector.begin(), new_vector.end());
=======
        std::vector<InputCppType> new_vector = std::move(this->data(state).items);
        for (auto& innerData : this->data(state).grid) {
            std::move(innerData.begin() + 1, innerData.end() - 1, std::back_inserter(new_vector));
        }

        pdqsort(new_vector.begin(), new_vector.end());
>>>>>>> 50051cf9a9 ([Enhancement] percentile_cont performance enhance (#36120))
        const double& rate = this->data(state).rate;

        ResultColumnType* column = down_cast<ResultColumnType*>(to);
        DCHECK(!new_vector.empty());
        if (new_vector.size() == 1 || rate == 1) {
            column->append(new_vector.back());
            return;
        }

        // choose the uppper one
        int index = ceil((new_vector.size() - 1) * rate);

        [[maybe_unused]] ResultType result;
        if constexpr (pt_is_datetime<PT>) {
            result.from_unix_second(new_vector[index].to_unix_second());
        } else if constexpr (pt_is_date<PT>) {
            result._julian = new_vector[index]._julian;
        } else if constexpr (pt_is_arithmetic<PT> || pt_is_string<PT> || pt_is_decimal_of_any_version<PT>) {
            result = new_vector[index];
        } else {
            // won't go there if percentile_disc is registered correctly
            throw std::runtime_error("Invalid PrimitiveTypes for percentile_disc function");
        }

        column->append(result);
    }

    std::string get_name() const override { return "percentile_disc"; }
};

<<<<<<< HEAD
} // namespace starrocks::vectorized
=======
} // namespace starrocks
>>>>>>> 50051cf9a9 ([Enhancement] percentile_cont performance enhance (#36120))
