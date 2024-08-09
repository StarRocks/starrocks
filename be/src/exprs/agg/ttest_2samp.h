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

#include <cctype>
#include <cmath>
#include <ios>
#include <iterator>
#include <limits>
#include <sstream>

#include "column/const_column.h"
#include "column/vectorized_fwd.h"
#include "delta_method.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/ttest_common.h"
#include "exprs/all_in_sql_functions.h"
#include "exprs/function_helper.h"
#include "gutil/casts.h"
#include "types/logical_type.h"

namespace starrocks {

using Ttest2SampAlternativeColumnType = RunTimeColumnType<TYPE_VARCHAR>;
using Ttest2SampDataMuColumnType = RunTimeColumnType<TYPE_DOUBLE>;
using Ttest2SampDataAlphaColumnType = RunTimeColumnType<TYPE_DOUBLE>;
using Ttest2SampDataArrayColumnType = RunTimeColumnType<TYPE_ARRAY>;
using Ttest2SampDataElementColumnType = RunTimeColumnType<TYPE_DOUBLE>;
using Ttest2SampTreatmentColumnType = RunTimeColumnType<TYPE_BOOLEAN>;
using Ttest2SampResultColumnType = RunTimeColumnType<TYPE_VARCHAR>;

class Ttest2SampParams {
public:
    bool operator==(const Ttest2SampParams& other) const {
        return _alternative == other._alternative && _alpha == other._alpha &&
               _cuped_expression == other._cuped_expression;
    }

    bool is_uninitialized() const { return _alternative == TtestAlternative::Unknown; }

    void reset() {
        _alternative = TtestAlternative::Unknown;
        _num_variables = -1;
        _alpha = TtestCommon::kDefaultAlphaValue;
        std::string().swap(_Y_expression);
        std::string().swap(_cuped_expression);
    }

    void init(TtestAlternative alternative, int num_variables, std::string const& Y_expression,
              std::string const& cuped_expression, double alpha) {
        _alternative = alternative;
        _num_variables = num_variables;
        _alpha = alpha;
        _Y_expression = Y_expression;
        _cuped_expression = cuped_expression;
    }

    void serialize(uint8_t*& data) const {
        SerializeHelpers::serialize(reinterpret_cast<const uint8_t*>(&_alternative), data);
        if (is_uninitialized()) {
            return;
        }
        SerializeHelpers::serialize(&_num_variables, data);
        SerializeHelpers::serialize(&_alpha, data);
        uint32_t cuped_expression_length = _cuped_expression.length();
        SerializeHelpers::serialize(&cuped_expression_length, data);
        SerializeHelpers::serialize(_cuped_expression.data(), data, cuped_expression_length);
        uint32_t Y_expression_length = _Y_expression.length();
        SerializeHelpers::serialize(&Y_expression_length, data);
        SerializeHelpers::serialize(_Y_expression.data(), data, Y_expression_length);
    }

    void deserialize(const uint8_t*& data) {
        SerializeHelpers::deserialize(data, reinterpret_cast<uint8_t*>(&_alternative));
        if (is_uninitialized()) {
            return;
        }
        SerializeHelpers::deserialize(data, &_num_variables);
        SerializeHelpers::deserialize(data, &_alpha);
        uint32_t cuped_expression_length;
        SerializeHelpers::deserialize(data, &cuped_expression_length);
        _cuped_expression.resize(cuped_expression_length);
        SerializeHelpers::deserialize(data, _cuped_expression.data(), cuped_expression_length);
        uint32_t Y_expression_length;
        SerializeHelpers::deserialize(data, &Y_expression_length);
        _Y_expression.resize(Y_expression_length);
        SerializeHelpers::deserialize(data, _Y_expression.data(), Y_expression_length);
    }

    size_t serialized_size() const {
        if (is_uninitialized()) {
            return sizeof(_alternative);
        }
        return sizeof(_alternative) + sizeof(_num_variables) + sizeof(_alpha) + sizeof(uint32_t) +
               _cuped_expression.length() + sizeof(uint32_t) + _Y_expression.length();
    }

    TtestAlternative alternative() const { return _alternative; }

    int num_variables() const { return _num_variables; }

    double alpha() const { return _alpha; }

    std::string const& Y_expression() const { return _Y_expression; }

    std::string const& cuped_expression() const { return _cuped_expression; }

private:
    TtestAlternative _alternative{TtestAlternative::Unknown};
    int _num_variables{-1};
    double _alpha{0.05};
    std::string _Y_expression;
    std::string _cuped_expression;
};

class Ttest2SampAggregateState {
public:
    Ttest2SampAggregateState() = default;
    Ttest2SampAggregateState(const Ttest2SampAggregateState&) = delete;
    Ttest2SampAggregateState(Ttest2SampAggregateState&&) = delete;

    Ttest2SampAggregateState(const uint8_t* serialized_data) { deserialize(serialized_data); }

    bool is_uninitialized() const {
        return _ttest_params.is_uninitialized() || _delta_method_stats0.is_uninitialized() ||
               _delta_method_stats1.is_uninitialized();
    }

    void check_params(Ttest2SampAggregateState const& other) const { DCHECK(_ttest_params == other._ttest_params); }

    void init(TtestAlternative alternative, int num_variables, std::string const& Y_expression,
              std::optional<std::string> const& cuped_expression, std::optional<double> alpha) {
        _ttest_params.init(alternative, num_variables, Y_expression, cuped_expression.value_or(std::string{}),
                           alpha.value_or(TtestCommon::kDefaultAlphaValue));
        if (!_ttest_params.is_uninitialized()) {
            _delta_method_stats0.init(num_variables);
            _delta_method_stats1.init(num_variables);
        }
    }

    void update(const double* input, int num_variables, bool treatment) {
        CHECK(!is_uninitialized() && !_delta_method_stats0.is_uninitialized() &&
              !_delta_method_stats1.is_uninitialized());
        if (treatment) {
            _delta_method_stats1.update(input, num_variables);
        } else {
            _delta_method_stats0.update(input, num_variables);
        }
    }

    void merge(Ttest2SampAggregateState const& other) {
        if (other.is_uninitialized()) {
            reset();
            return;
        }
        check_params(other);
        _delta_method_stats0.merge(other._delta_method_stats0);
        _delta_method_stats1.merge(other._delta_method_stats1);
    }

    void serialize(uint8_t*& data) const {
        _ttest_params.serialize(data);
        if (_ttest_params.is_uninitialized()) {
            return;
        }
        DCHECK(!_delta_method_stats0.is_uninitialized());
        _delta_method_stats0.serialize(data);
        DCHECK(!_delta_method_stats1.is_uninitialized());
        _delta_method_stats1.serialize(data);
    }

    void deserialize(const uint8_t*& data) {
        _ttest_params.deserialize(data);
        if (_ttest_params.is_uninitialized()) {
            return;
        }
        _delta_method_stats0.init(_ttest_params.num_variables());
        _delta_method_stats0.deserialize(data);
        _delta_method_stats1.init(_ttest_params.num_variables());
        _delta_method_stats1.deserialize(data);
    }

    void reset() {
        _ttest_params.reset();
        _delta_method_stats0.reset();
        _delta_method_stats1.reset();
    }

    size_t serialized_size() const {
        size_t size = _ttest_params.serialized_size();
        if (_ttest_params.is_uninitialized()) {
            return size;
        }
        DCHECK(!_delta_method_stats0.is_uninitialized());
        DCHECK(!_delta_method_stats1.is_uninitialized());
        return size + _delta_method_stats0.serialized_size() + _delta_method_stats1.serialized_size();
    }

    std::string get_ttest_result() const {
        if (is_uninitialized()) {
            return fmt::format("Internal error: ttest agg state is uninitialized.");
        }
        if (_delta_method_stats0.count() <= 1) {
            return fmt::format("count({}) of group 0 should be greater than 1.", _delta_method_stats0.count());
        }
        if (_delta_method_stats1.count() <= 1) {
            return fmt::format("count({}) of group 1 should be greater than 1.", _delta_method_stats1.count());
        }

        DeltaMethodStats delta_method_stats;
        delta_method_stats.init(_ttest_params.num_variables());
        delta_method_stats.merge(_delta_method_stats0);
        delta_method_stats.merge(_delta_method_stats1);

        double mean0 = 0, mean1 = 0, var0 = 0, var1 = 0;

        if (!TtestCommon::calc_means_and_vars(
                    _ttest_params.Y_expression(), _ttest_params.cuped_expression(), _ttest_params.num_variables(),
                    _delta_method_stats0.count(), _delta_method_stats1.count(), _delta_method_stats0.means(),
                    _delta_method_stats1.means(), delta_method_stats.means(), _delta_method_stats0.cov_matrix(),
                    _delta_method_stats1.cov_matrix(), delta_method_stats.cov_matrix(), mean0, mean1, var0, var1)) {
            return "InvertMatrix failed. some variables in the table are perfectly collinear.";
        }

        double stderr_var = std::sqrt(var0 + var1);

        if (!std::isfinite(stderr_var)) {
            return fmt::format("stderr({}) is an abnormal float value, please check your data.", stderr_var);
        }

        double estimate = mean1 - mean0;
        double t_stat = estimate / stderr_var;
        size_t count = _delta_method_stats0.count() + _delta_method_stats1.count();

        double p_value = TtestCommon::calc_pvalue(t_stat, _ttest_params.alternative());
        auto [lower, upper] = TtestCommon::calc_confidence_interval(estimate, stderr_var, count, _ttest_params.alpha(),
                                                                    _ttest_params.alternative());

        std::stringstream result_ss;
        result_ss << "\n";
        result_ss << MathHelpers::to_string_with_precision("mean0");
        result_ss << MathHelpers::to_string_with_precision("mean1");
        result_ss << MathHelpers::to_string_with_precision("estimate");
        result_ss << MathHelpers::to_string_with_precision("stderr");
        result_ss << MathHelpers::to_string_with_precision("t-statistic");
        result_ss << MathHelpers::to_string_with_precision("p-value");
        result_ss << MathHelpers::to_string_with_precision("lower");
        result_ss << MathHelpers::to_string_with_precision("upper");
        result_ss << "\n";
        result_ss << MathHelpers::to_string_with_precision(mean0);
        result_ss << MathHelpers::to_string_with_precision(mean1);
        result_ss << MathHelpers::to_string_with_precision(estimate);
        result_ss << MathHelpers::to_string_with_precision(stderr_var);
        result_ss << MathHelpers::to_string_with_precision(t_stat);
        result_ss << MathHelpers::to_string_with_precision(p_value);
        result_ss << MathHelpers::to_string_with_precision(lower);
        result_ss << MathHelpers::to_string_with_precision(upper);
        result_ss << "\n";

        return result_ss.str();
    }

private:
    Ttest2SampParams _ttest_params;
    DeltaMethodStats _delta_method_stats0;
    DeltaMethodStats _delta_method_stats1;
};

class Ttest2SampAggregateFunction
        : public AggregateFunctionBatchHelper<Ttest2SampAggregateState, Ttest2SampAggregateFunction> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        // expression, side, treatment, data, [cuped, alpha]
        const Column* data_col = columns[3];
        auto [input, array_size] =
                FunctionHelper::get_data_of_array<DeltaMethodDataElementColumnType, double>(data_col, row_num);
        if (input == nullptr) {
            ctx->set_error("Internal Error: fail to get data.");
            return;
        }

        if (this->data(state).is_uninitialized()) {
            const Column* expr_col = columns[0];
            const auto* func_expr = down_cast<const DeltaMethodExprColumnType*>(expr_col);
            Slice expr_slice = func_expr->get_data()[0];
            std::string expression = expr_slice.to_string();
            std::optional<std::string> cuped_expression;
            std::optional<double> alpha;

            if (ctx->get_num_args() >= 5) {
                cuped_expression = try_parse_cuped(columns[4]);
                alpha = try_parse_alpha(columns[4]);
            }

            if (ctx->get_num_args() >= 6) {
                if (!cuped_expression.has_value()) {
                    cuped_expression = try_parse_cuped(columns[5]);
                }
                if (!alpha.has_value()) {
                    alpha = try_parse_alpha(columns[5]);
                }
            }

            const Column* alternative_col = columns[1];
            const auto* alternative_column =
                    FunctionHelper::unwrap_if_const<const Ttest2SampAlternativeColumnType*>(alternative_col);
            if (alternative_col == nullptr) {
                ctx->set_error("Internal Error: fail to get alternative.");
                return;
            }
            std::string alternative_str = alternative_column->get_data()[0].to_string();
            if (!TtestCommon::str2alternative.count(alternative_str)) {
                ctx->set_error(fmt::format("Invalid Argument: alternative({}) is not a valid ttest alternative.",
                                           alternative_str)
                                       .c_str());
                return;
            }
            TtestAlternative alternative = TtestCommon::str2alternative.at(alternative_str);

            LOG(INFO) << fmt::format("ttest args - expression: {}, alternative: {}, cuped_expression: {}, alpha: {}",
                                     expression, (int)alternative, cuped_expression.value_or("null"),
                                     alpha.value_or(TtestCommon::kDefaultAlphaValue));
            this->data(state).init(alternative, array_size, expression, cuped_expression, alpha);
        }

        const Column* treatment_col = columns[2];
        const auto* treatment_column =
                FunctionHelper::unwrap_if_nullable<const Ttest2SampTreatmentColumnType*>(treatment_col, row_num);
        CHECK(treatment_col != nullptr);
        double treatment = treatment_column->get_data()[row_num];

        this->data(state).update(input, array_size, treatment);
    }

    std::optional<std::string> try_parse_cuped(const Column* cuped_expr_col) const {
        cuped_expr_col = FunctionHelper::unwrap_if_const<const Column*>(cuped_expr_col);
        if (cuped_expr_col == nullptr) {
            return std::nullopt;
        }
        if (typeid(*cuped_expr_col) != typeid(DeltaMethodExprColumnType)) {
            return std::nullopt;
        }
        const auto* cuped_func_expr = down_cast<const DeltaMethodExprColumnType*>(cuped_expr_col);
        std::string cuped_expr = cuped_func_expr->get_data()[0].to_string();
        std::string const prefix = "X=";
        size_t prefix_length = prefix.length();
        if (prefix != cuped_expr.substr(0, prefix_length)) {
            return std::nullopt;
        }
        return cuped_expr.substr(prefix_length);
    }

    std::optional<double> try_parse_alpha(const Column* alpha_col) const {
        alpha_col = FunctionHelper::unwrap_if_const<const Column*>(alpha_col);
        if (alpha_col == nullptr) {
            return std::nullopt;
        }
        if (typeid(*alpha_col) != typeid(Ttest2SampDataAlphaColumnType)) {
            return std::nullopt;
        }
        const auto* alpha_column = down_cast<const Ttest2SampDataAlphaColumnType*>(alpha_col);
        return alpha_column->get_data()[0];
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        column = FunctionHelper::unwrap_if_nullable<const Column*>(column, row_num);
        if (column == nullptr) {
            ctx->set_error("Internal Error: fail to get intermediate data.");
            return;
        }
        DCHECK(column->is_binary());
        const uint8_t* serialized_data = reinterpret_cast<const uint8_t*>(column->get(row_num).get_slice().data);
        if (this->data(state).is_uninitialized()) {
            this->data(state).deserialize(serialized_data);
            return;
        }
        Ttest2SampAggregateState other(serialized_data);
        this->data(state).merge(other);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()) {
            auto* dst_nullable_col = down_cast<NullableColumn*>(to);
            dst_nullable_col->null_column_data().emplace_back(false);
            to = dst_nullable_col->data_column().get();
        }
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
        if (to->is_nullable()) {
            auto* dst_nullable_col = down_cast<NullableColumn*>(to);
            if (this->data(state).is_uninitialized()) {
                ctx->set_error("Internal Error: state not initialized.");
                return;
            }
            dst_nullable_col->null_column_data().emplace_back(false);
            to = dst_nullable_col->data_column().get();
        }
        std::string result;
        if (this->data(state).is_uninitialized()) {
            ctx->set_error("Internal Error: state not initialized.");
            return;
        }
        result = this->data(state).get_ttest_result();
        down_cast<Ttest2SampResultColumnType*>(to)->append(result);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {}

    std::string get_name() const override { return "ttest_2samp"; }
};

} // namespace starrocks
