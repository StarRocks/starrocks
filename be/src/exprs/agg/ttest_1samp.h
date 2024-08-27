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
#include "exprs/function_helper.h"
#include "gutil/casts.h"
#include "types/logical_type.h"

namespace starrocks {

using DeltaMethodExprColumnType = RunTimeColumnType<TYPE_VARCHAR>;
using Ttest1SampAlternativeColumnType = RunTimeColumnType<TYPE_VARCHAR>;
using Ttest1SampDataMuColumnType = RunTimeColumnType<TYPE_DOUBLE>;
using Ttest1SampDataAlphaColumnType = RunTimeColumnType<TYPE_DOUBLE>;
using Ttest1SampDataArrayColumnType = RunTimeColumnType<TYPE_ARRAY>;
using Ttest1SampDataElementColumnType = RunTimeColumnType<TYPE_DOUBLE>;
using Ttest1SampResultColumnType = RunTimeColumnType<TYPE_VARCHAR>;

class Ttest1SampParams {
public:
    bool operator==(const Ttest1SampParams& other) const {
        return _alternative == other._alternative && _mu == other._mu && _alpha == other._alpha &&
               _cuped_expression == other._cuped_expression;
    }

    bool is_uninitialized() const { return _alternative == TtestAlternative::Unknown; }

    void reset() {
        _alternative = TtestAlternative::Unknown;
        _num_variables = -1;
        _mu = 0;
        _alpha = TtestCommon::kDefaultAlphaValue;
        std::string().swap(_Y_expression);
        std::string().swap(_cuped_expression);
    }

    void init(TtestAlternative alternative, int num_variables, double mu, std::string const& Y_expression,
              std::string const& cuped_expression, double alpha) {
        _alternative = alternative;
        _num_variables = num_variables;
        _mu = mu;
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
        SerializeHelpers::serialize(&_mu, data);
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
        SerializeHelpers::deserialize(data, &_mu);
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
        return sizeof(_alternative) + sizeof(_num_variables) + sizeof(_mu) + sizeof(_alpha) + sizeof(uint32_t) +
               _cuped_expression.length() + sizeof(uint32_t) + _Y_expression.length();
    }

    TtestAlternative alternative() const { return _alternative; }

    int num_variables() const { return _num_variables; }

    double mu() const { return _mu; }

    double alpha() const { return _alpha; }

    std::string const& Y_expression() const { return _Y_expression; }

    std::string const& cuped_expression() const { return _cuped_expression; }

private:
    TtestAlternative _alternative{TtestAlternative::Unknown};
    int _num_variables{-1};
    double _mu{0};
    double _alpha{0.05};
    std::string _Y_expression;
    std::string _cuped_expression;
};

class Ttest1SampAggregateState {
public:
    Ttest1SampAggregateState() = default;
    Ttest1SampAggregateState(const Ttest1SampAggregateState&) = delete;
    Ttest1SampAggregateState(Ttest1SampAggregateState&&) = delete;

    Ttest1SampAggregateState(const uint8_t* serialized_data) { deserialize(serialized_data); }

    bool is_uninitialized() const { return _ttest_params.is_uninitialized() || _delta_method_stats.is_uninitialized(); }

    void check_params(Ttest1SampAggregateState const& other) const { DCHECK(_ttest_params == other._ttest_params); }

    void init(TtestAlternative alternative, int num_variables, double mu, std::string const& Y_expression,
              std::optional<std::string> const& cuped_expression, std::optional<double> alpha) {
        _ttest_params.init(alternative, num_variables, mu, Y_expression, cuped_expression.value_or(std::string{}),
                           alpha.value_or(TtestCommon::kDefaultAlphaValue));
        if (!_ttest_params.is_uninitialized()) {
            _delta_method_stats.init(num_variables);
        }
    }

    void update(const double* input, int num_variables) {
        CHECK(!is_uninitialized() && !_delta_method_stats.is_uninitialized());
        _delta_method_stats.update(input, num_variables);
    }

    void merge(Ttest1SampAggregateState const& other) {
        if (other.is_uninitialized()) {
            reset();
            return;
        }
        check_params(other);
        _delta_method_stats.merge(other._delta_method_stats);
    }

    void serialize(uint8_t*& data) const {
        _ttest_params.serialize(data);
        if (_ttest_params.is_uninitialized()) {
            return;
        }
        DCHECK(!_delta_method_stats.is_uninitialized());
        _delta_method_stats.serialize(data);
    }

    void deserialize(const uint8_t*& data) {
        _ttest_params.deserialize(data);
        if (_ttest_params.is_uninitialized()) {
            return;
        }
        _delta_method_stats.init(_ttest_params.num_variables());
        _delta_method_stats.deserialize(data);
    }

    void reset() {
        _ttest_params.reset();
        _delta_method_stats.reset();
    }

    size_t serialized_size() const {
        size_t size = _ttest_params.serialized_size();
        if (_ttest_params.is_uninitialized()) {
            return size;
        }
        DCHECK(!_delta_method_stats.is_uninitialized());
        return size + _delta_method_stats.serialized_size();
    }

    std::string get_ttest_result() const {
        DCHECK(!is_uninitialized());
        if (_delta_method_stats.count() <= 1) {
            return fmt::format("count({}) should be greater than 1.", _delta_method_stats.count());
        }
        auto means = _delta_method_stats.means();
        auto cov_matrix = _delta_method_stats.cov_matrix();
        size_t count = _delta_method_stats.count();
        double mean, var;
        if (!TtestCommon::calc_cuped_mean_and_var(_ttest_params.Y_expression(), _ttest_params.cuped_expression(),
                                                  _ttest_params.num_variables(), count, means, cov_matrix, mean, var)) {
            return "InvertMatrix failed. some variables in the table are perfectly collinear.";
        }

        double stderr_var = std::sqrt(var);
        if (!std::isfinite(stderr_var)) {
            return fmt::format("stderr({}) is an abnormal float value, please check your data.", stderr_var);
        }

        double estimate = mean - _ttest_params.mu();
        double t_stat = estimate / stderr_var;
        double p_value = TtestCommon::calc_pvalue(t_stat, _ttest_params.alternative());
        auto [lower, upper] = TtestCommon::calc_confidence_interval(estimate, stderr_var, count, _ttest_params.alpha(),
                                                                    _ttest_params.alternative());

        std::stringstream result_ss;
        result_ss << "\n";
        result_ss << MathHelpers::to_string_with_precision("estimate");
        result_ss << MathHelpers::to_string_with_precision("stderr");
        result_ss << MathHelpers::to_string_with_precision("t-statistic");
        result_ss << MathHelpers::to_string_with_precision("p-value");
        result_ss << MathHelpers::to_string_with_precision("lower");
        result_ss << MathHelpers::to_string_with_precision("upper");
        result_ss << "\n";
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
    Ttest1SampParams _ttest_params;
    DeltaMethodStats _delta_method_stats;
};

class Ttest1SampAggregateFunction
        : public AggregateFunctionBatchHelper<Ttest1SampAggregateState, Ttest1SampAggregateFunction> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
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
                try_parse_cuped_or_alpha(columns[4], cuped_expression, alpha);
            }
            if (ctx->get_num_args() >= 6) {
                if (!alpha.has_value()) {
                    alpha = try_parse_alpha(columns[5]);
                }
            }

            const Column* alternative_col = columns[1];
            const auto* alternative_column =
                    FunctionHelper::unwrap_if_const<const Ttest1SampAlternativeColumnType*>(alternative_col);
            if (alternative_column == nullptr) {
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

            const Column* mu_col = columns[2];
            const auto* mu_column = FunctionHelper::unwrap_if_const<const Ttest1SampDataMuColumnType*>(mu_col);
            if (mu_column == nullptr) {
                ctx->set_error("Internal Error: fail to get mu.");
                return;
            }
            double mu = mu_column->get_data()[0];

            LOG(INFO) << fmt::format(
                    "ttest args - expression: {}, alternative: {}, mu: {}, cuped_expression: {}, alpha: {}", expression,
                    (int)alternative, mu, cuped_expression.value_or("null"),
                    alpha.value_or(TtestCommon::kDefaultAlphaValue));
            this->data(state).init(alternative, array_size, mu, expression, cuped_expression, alpha);
        }

        this->data(state).update(input, array_size);
    }

    void try_parse_cuped_or_alpha(const Column* col, std::optional<std::string>& cuped_expression,
                                  std::optional<double>& alpha) const {
        col = FunctionHelper::unwrap_if_const<const Column*>(col);
        if (col == nullptr) {
            return;
        }
        if (typeid(*col) != typeid(DeltaMethodExprColumnType)) {
            return;
        }
        const auto* str_col = down_cast<const DeltaMethodExprColumnType*>(col);
        std::string value = str_col->get_data()[0].to_string();
        std::string const prefix = "X=";
        size_t prefix_length = prefix.length();
        if (prefix != value.substr(0, prefix_length)) {
            char* end = nullptr;
            double alpha_value = std::strtod(value.c_str(), &end);
            if (end != value.c_str()) {
                alpha = alpha_value;
            }
            return;
        }
        cuped_expression = value.substr(prefix_length);
    }

    std::optional<double> try_parse_alpha(const Column* alpha_col) const {
        alpha_col = FunctionHelper::unwrap_if_const<const Column*>(alpha_col);
        if (alpha_col == nullptr) {
            return std::nullopt;
        }
        if (typeid(*alpha_col) != typeid(Ttest1SampDataAlphaColumnType)) {
            return std::nullopt;
        }
        const auto* alpha_column = down_cast<const Ttest1SampDataAlphaColumnType*>(alpha_col);
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
        Ttest1SampAggregateState other(serialized_data);
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
        if (this->data(state).is_uninitialized()) {
            ctx->set_error("Internal Error: state not initialized.");
            return;
        }
        std::string result = this->data(state).get_ttest_result();
        down_cast<Ttest1SampResultColumnType*>(to)->append(result);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {}

    std::string get_name() const override { return "ttest_1samp"; }
};

} // namespace starrocks
