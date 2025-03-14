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

#pragma once

#include <velocypack/Builder.h>
#include <velocypack/Value.h>

#include <algorithm>
#include <boost/algorithm/string/case_conv.hpp>
#include <cctype>
#include <cmath>
#include <iterator>

#include "boost/math/distributions/normal.hpp"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/helpers/serialize_helpers.hpp"
#include "exprs/agg/hypothesis_testing_common.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "gutil/casts.h"
#include "types/logical_type.h"
#include "util/json.h"

namespace starrocks {

class MannWhitneyAggregateState {
public:
    MannWhitneyAggregateState() = default;
    MannWhitneyAggregateState(const uint8_t*& data) { deserialize(data); }

    void init(TestingAlternative alternative, int64_t continuity_correction) {
        _alternative = alternative;
        _continuity_correction = continuity_correction;
    }

    bool is_uninitialized() const { return _alternative == TestingAlternative::Unknown; }

    void update(double x, bool treatment) {
        _stats[treatment].emplace_back(x);
        _sorted = false;
    }

    void merge(MannWhitneyAggregateState const& other) {
        DCHECK(_alternative == other._alternative);
        sort_if_not_sorted();
        other.sort_if_not_sorted();
        for (size_t idx = 0; idx < 2; ++idx) {
            std::vector<double> tmp;
            std::merge(_stats[idx].begin(), _stats[idx].end(), other._stats[idx].begin(), other._stats[idx].end(),
                       std::back_inserter(tmp));
            _stats[idx] = tmp;
        }
    }

    void serialize(uint8_t*& data) const {
        DCHECK(!is_uninitialized());
        sort_if_not_sorted();
        SerializeHelpers::serialize_all(data, _alternative.value(), _stats, _continuity_correction);
    }

    void deserialize(const uint8_t*& data) {
        uint8_t tmp;
        SerializeHelpers::deserialize_all(data, tmp, _stats, _continuity_correction);
        _alternative = TestingAlternative(tmp);
        _sorted = true;
    }

    size_t serialized_size() const {
        return SerializeHelpers::serialized_size_all(_alternative.value(), _stats, _continuity_correction);
    }

    void build_result(vpack::Builder& builder) const {
        if (_alternative == TestingAlternative::Unknown) {
            vpack::ObjectBuilder obj_builder(&builder);
            builder.add("Logical Error", vpack::Value("state not initialized."));
            return;
        }
        // If there's only one state, it's not sorted yet, so sorted it.
        sort_if_not_sorted();
        size_t size = _stats[0].size() + _stats[1].size();
        std::vector<size_t> index(size);
        std::iota(index.begin(), index.end(), 0);
        auto data = [this](size_t idx) {
            if (idx < this->_stats[0].size()) {
                return this->_stats[0][idx];
            }
            return this->_stats[1][idx - this->_stats[0].size()];
        };
        std::inplace_merge(index.begin(), index.begin() + _stats[0].size(), index.end(),
                           [data](size_t lhs, size_t rhs) { return data(lhs) < data(rhs); });
        DCHECK(std::is_sorted(index.begin(), index.end(),
                              [data](size_t lhs, size_t rhs) { return data(lhs) < data(rhs); }));

        const double n1 = _stats[0].size();
        const double n2 = _stats[1].size();
        double r1 = 0;
        double tie_correction = 0;
        size_t left = 0;
        double tie_numenator = 0;
        while (left < size) {
            size_t right = left;
            while (right < size && data(index[left]) == data(index[right])) {
                ++right;
            }
            auto adjusted = (left + right + 1.) / 2.;
            auto count_equal = right - left;

            // Scipy implementation throws exception in this case too.
            if (count_equal == size) {
                vpack::ObjectBuilder obj_builder(&builder);
                builder.add("Error", vpack::Value("All numbers in both samples are identical."));
                return;
            }

            tie_numenator += std::pow(count_equal, 3) - count_equal;
            for (size_t iter = left; iter < right; ++iter) {
                if (index[iter] < n1) {
                    r1 += adjusted;
                }
            }
            left = right;
        }
        tie_correction = 1 - (tie_numenator / (std::pow(size, 3) - size));

        const double u1 = n1 * n2 + (n1 * (n1 + 1.)) / 2. - r1;
        const double u2 = n1 * n2 - u1;

        /// The distribution of U-statistic under null hypothesis H0  is symmetric with respect to meanrank.
        const double meanrank = n1 * n2 / 2. + 0.5 * _continuity_correction;
        const double sd = std::sqrt(tie_correction * n1 * n2 * (n1 + n2 + 1) / 12.0);

        if (std::isnan(sd) || std::isinf(sd) || std::abs(sd) < 1e-7) {
            vpack::ObjectBuilder obj_builder(&builder);
            builder.add("Logical Error", vpack::Value(fmt::format("sd({}) is not a valid value.", sd)));
            return;
        }

        double u = 0;
        if (_alternative == TestingAlternative::TwoSided) {
            u = std::max(u1, u2);
        } else if (_alternative == TestingAlternative::Less) {
            u = u1;
        } else if (_alternative == TestingAlternative::Greater) {
            u = u2;
        } else {
            DCHECK(false);
        }

        double z = (u - meanrank) / sd;
        if (_alternative == TestingAlternative::TwoSided) {
            z = std::abs(z);
        }

        auto standart_normal_distribution = boost::math::normal_distribution<double>();
        auto cdf = boost::math::cdf(standart_normal_distribution, z);

        double p_value = 0;
        if (_alternative == TestingAlternative::TwoSided) {
            p_value = 2 - 2 * cdf;
        } else {
            p_value = 1 - cdf;
        }

        vpack::ArrayBuilder array_builder(&builder);
        builder.add(vpack::Value(u2));
        builder.add(vpack::Value(p_value));
    }

private:
    TestingAlternative _alternative;
    mutable std::array<std::vector<double>, 2> _stats;
    int64_t _continuity_correction{0};
    mutable bool _sorted{true};

    void sort_if_not_sorted() const {
        if (!_sorted) {
            for (size_t idx = 0; idx < 2; ++idx) {
                std::sort(_stats[idx].begin(), _stats[idx].end());
            }
            _sorted = true;
        }
        for (size_t idx = 0; idx < 2; ++idx) {
            DCHECK(std::is_sorted(_stats[idx].begin(), _stats[idx].end()));
        }
    }
};

class MannWhitneyUTestAggregateFunction final
        : public AggregateFunctionBatchHelper<MannWhitneyAggregateState, MannWhitneyUTestAggregateFunction> {
public:
    using DataColumn = RunTimeColumnType<TYPE_DOUBLE>;
    using DataCppType = RunTimeCppType<TYPE_DOUBLE>;
    using IndexColumn = RunTimeColumnType<TYPE_BOOLEAN>;
    using AlternativeColumn = RunTimeColumnType<TYPE_VARCHAR>;
    using ContinuityCorrectionColumn = RunTimeColumnType<TYPE_BIGINT>;
    using ResultColumn = RunTimeColumnType<TYPE_JSON>;

    void init_state_if_needed(FunctionContext* ctx, const Column* alternative_col,
                              const Column* continuity_correction_col, MannWhitneyAggregateState& state) const {
        if (!state.is_uninitialized()) {
            return;
        }
        init_state(ctx, alternative_col, continuity_correction_col, state);
    }

    void init_state(FunctionContext* ctx, const Column* alternative_col, const Column* continuity_correction_col,
                    MannWhitneyAggregateState& state) const {
        TestingAlternative alternative{TestingAlternative::TwoSided};
        int64_t continuity_correction = 1;

        if (alternative_col != nullptr) {
            Slice alternative_slice;
            FunctionHelper::get_data_of_column<AlternativeColumn>(alternative_col, 0, alternative_slice);
            auto alternative_str = boost::to_lower_copy(alternative_slice.to_string());
            TestingAlternative init_alternative = TestingAlternative::from_str(alternative_str);
            if (init_alternative == TestingAlternative::Unknown) {
                ctx->set_error(fmt::format("Logical Error: invalid alternative `{}`.", alternative_str).c_str());
                return;
            }
            alternative = init_alternative;
        }

        if (continuity_correction_col != nullptr) {
            FunctionHelper::get_data_of_column<ContinuityCorrectionColumn>(continuity_correction_col, 0,
                                                                           continuity_correction);
            if (continuity_correction < 0) {
                ctx->set_error("Logical Error: continuity_correction must be non-negative.");
                return;
            }
        }

        state.init(alternative, continuity_correction);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const Column* alternative_col = ctx->get_num_args() >= 3 ? columns[2] : nullptr;
        const Column* continuity_correction_col = ctx->get_num_args() >= 4 ? columns[3] : nullptr;
        init_state_if_needed(ctx, alternative_col, continuity_correction_col, this->data(state));

        DataCppType x{};
        const Column* x_col = columns[0];
        FunctionHelper::get_data_of_column<DataColumn>(x_col, row_num, x);

        if (std::isnan((double)x) || std::isinf((double)x)) {
            return;
        }

        bool treatment = false;
        const Column* treatment_col = columns[1];
        FunctionHelper::get_data_of_column<IndexColumn>(treatment_col, row_num, treatment);

        this->data(state).update(x, treatment);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        const uint8_t* serialized_data = reinterpret_cast<const uint8_t*>(column->get(row_num).get_slice().data);
        if (this->data(state).is_uninitialized()) {
            this->data(state).deserialize(serialized_data);
            return;
        }
        MannWhitneyAggregateState other(serialized_data);
        this->data(state).merge(other);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();
        size_t old_size = bytes.size();
        size_t new_size = old_size + this->data(state).serialized_size();
        bytes.resize(new_size);
        column->get_offset().emplace_back(new_size);
        uint8_t* serialized_data = bytes.data() + old_size;
        this->data(state).serialize(serialized_data);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (this->data(state).is_uninitialized()) {
            ctx->set_error("Internal Error: state not initialized.");
            return;
        }
        vpack::Builder result_builder;
        this->data(state).build_result(result_builder);
        JsonValue result_json(result_builder.slice());
        down_cast<ResultColumn*>(to)->append(std::move(result_json));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        TestingAlternative alternative{TestingAlternative::TwoSided};
        int64_t continuity_correction = 1;

        const Column* alternative_col = ctx->get_num_args() >= 3 ? src[2].get() : nullptr;
        const Column* continuity_correction_col = ctx->get_num_args() >= 4 ? src[3].get() : nullptr;
        if (alternative_col != nullptr) {
            Slice alternative_slice;
            FunctionHelper::get_data_of_column<AlternativeColumn>(alternative_col, 0, alternative_slice);
            auto alternative_str = boost::to_lower_copy(alternative_slice.to_string());
            TestingAlternative init_alternative = TestingAlternative::from_str(alternative_str);
            if (init_alternative == TestingAlternative::Unknown) {
                ctx->set_error(fmt::format("Logical Error: invalid alternative `{}`.", alternative_str).c_str());
                return;
            }
            alternative = init_alternative;
        }

        if (continuity_correction_col != nullptr) {
            FunctionHelper::get_data_of_column<ContinuityCorrectionColumn>(continuity_correction_col, 0,
                                                                           continuity_correction);
            if (continuity_correction < 0) {
                ctx->set_error("Logical Error: continuity_correction must be non-negative.");
                return;
            }
        }

        for (size_t idx = 0; idx < chunk_size; ++idx) {
            MannWhitneyAggregateState state;
            state.init(alternative, continuity_correction);

            DataCppType x{};
            const Column* x_col = src[0].get();
            FunctionHelper::get_data_of_column<DataColumn>(x_col, idx, x);

            bool treatment = false;
            const Column* treatment_col = src[1].get();
            FunctionHelper::get_data_of_column<IndexColumn>(treatment_col, idx, treatment);

            if (!(std::isnan((double)x) || std::isinf((double)x))) {
                state.update(x, treatment);
            }

            serialize_to_column(ctx, reinterpret_cast<AggDataPtr>(&state), dst->get());
        }
    }

    std::string get_name() const override { return "mann_whitney_u_test"; }
};

} // namespace starrocks
