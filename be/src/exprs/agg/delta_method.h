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

#include <glog/logging.h>

#include <boost/numeric/ublas/matrix.hpp>
#include <boost/numeric/ublas/triangular.hpp>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <sstream>
#include <unordered_map>

#include "column/type_traits.h"
#include "common/status.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_helper.h"
#include "exprs/helpers/expr_tree.hpp"
#include "exprs/helpers/serialize_helpers.hpp"
#include "gutil/casts.h"
#include "storage/types.h"
#include "types/logical_type.h"
#include "util/slice.h"

namespace starrocks {

using DeltaMethodExprColumnType = RunTimeColumnType<TYPE_VARCHAR>;
using DeltaMethodIsStdColumnType = RunTimeColumnType<TYPE_BOOLEAN>;
using DeltaMethodDataArrayColumnType = RunTimeColumnType<TYPE_ARRAY>;
using DeltaMethodDataElementColumnType = RunTimeColumnType<TYPE_DOUBLE>;
using DeltaMethodResultColumnType = RunTimeColumnType<TYPE_DOUBLE>;

namespace ublas = boost::numeric::ublas;

class DeltaMethodParams {
public:
    bool operator==(const DeltaMethodParams& other) const {
        return _num_variables == other._num_variables && _expression == other._expression && _is_std == other._is_std;
    }

    bool is_uninitialized() const { return _num_variables == -1; }

    void reset() {
        _num_variables = -1;
        std::string().swap(_expression);
        _is_std = true;
    }

    void init(int num_cols, std::string const& expr, bool is_std) {
        _num_variables = num_cols;
        _expression = expr;
        _is_std = is_std;
    }

    void serialize(uint8_t*& data) const {
        SerializeHelpers::serialize(&_num_variables, data);
        uint32_t expression_length = _expression.length();
        SerializeHelpers::serialize(&expression_length, data);
        SerializeHelpers::serialize(_expression.data(), data, expression_length);
        SerializeHelpers::serialize(&_is_std, data);
    }

    void deserialize(const uint8_t*& data) {
        SerializeHelpers::deserialize(data, &_num_variables);
        uint32_t expression_length = -1;
        SerializeHelpers::deserialize(data, &expression_length);
        _expression.resize(expression_length);
        SerializeHelpers::deserialize(data, _expression.data(), expression_length);
        SerializeHelpers::deserialize(data, &_is_std);
    }

    size_t serialized_size() const {
        return sizeof(_num_variables) + sizeof(uint32_t) + _expression.length() * sizeof(char) + sizeof(_is_std);
    }

    int num_variables() const { return _num_variables; }

    std::string const& expression() const { return _expression; }

    bool is_stderr() const { return _is_std; }

private:
    int _num_variables{-1};
    std::string _expression;
    uint8_t _is_std{true};
};

class DeltaMethodStats {
public:
    DeltaMethodStats() = default;
    DeltaMethodStats(const DeltaMethodStats&) = delete;
    DeltaMethodStats(DeltaMethodStats&&) = delete;

    bool is_uninitialized() const { return _num_variables == -1; }

    void init(int length) {
        CHECK(length >= 0);
        _num_variables = length;
        _count = 0;
        _sum_x = ublas::vector<double>(length, 0);
        _sum_xy = ublas::triangular_matrix<double, ublas::upper>(length, length);
        std::fill(_sum_xy.data().begin(), _sum_xy.data().end(), 0);
    }

    void reset() {
        _num_variables = -1;
        _count = 0;
        ublas::vector<double>().swap(_sum_x);
        ublas::triangular_matrix<double, ublas::upper>().swap(_sum_xy);
    }

    void update(const double* input, int length) {
        CHECK_EQ(_num_variables, length);
        for (uint32_t i = 0; i < _num_variables; ++i) {
            _sum_x(i) += input[i];
        }
        for (uint32_t i = 0; i < _num_variables; ++i) {
            for (uint32_t j = i; j < _num_variables; ++j) {
                _sum_xy(i, j) += input[i] * input[j];
            }
        }
        _count += 1;
    }

    void merge(DeltaMethodStats const& other) {
        CHECK_EQ(_num_variables, other._num_variables);
        _sum_x += other._sum_x;
        _sum_xy += other._sum_xy;
        _count += other._count;
    }

    int num_variables() const { return _num_variables; }

    const ublas::vector<double> means() const {
        CHECK(_count > 0);
        return _sum_x / _count;
    }

    size_t count() const { return _count; }

    ublas::matrix<double> cov_matrix() const {
        CHECK(_count > 1);
        auto mean = means();
        ublas::matrix<double> cov_matrix(_num_variables, _num_variables);
        for (uint32_t i = 0; i < _num_variables; ++i) {
            for (uint32_t j = 0; j < _num_variables; ++j) {
                auto sum_xi_xj = i < j ? _sum_xy(i, j) : _sum_xy(j, i);
                cov_matrix(i, j) = (sum_xi_xj - _count * mean[i] * mean[j]) / (_count - 1);
            }
        }
        return cov_matrix;
    }

    void serialize(uint8_t*& data) const {
        CHECK(!is_uninitialized());
        SerializeHelpers::serialize(&_count, data);
        SerializeHelpers::serialize(_sum_x.data().begin(), data, _num_variables);
        SerializeHelpers::serialize(_sum_xy.data().begin(), data, _num_variables * (_num_variables + 1) / 2);
    }

    void deserialize(const uint8_t*& data) {
        CHECK(!is_uninitialized());
        SerializeHelpers::deserialize(data, &_count);
        SerializeHelpers::deserialize(data, _sum_x.data().begin(), _num_variables);
        SerializeHelpers::deserialize(data, _sum_xy.data().begin(), _num_variables * (_num_variables + 1) / 2);
    }

    size_t serialized_size() const {
        CHECK(!is_uninitialized());
        return sizeof(_count) + sizeof(double) * _num_variables +
               sizeof(double) * _num_variables * (_num_variables + 1) / 2;
    }

    static double calc_delta_method(ExprTree<double> const& Y_expr_tree, size_t count,
                                    ublas::vector<double> const& means, ublas::matrix<double> const& cov_matrix,
                                    bool is_std) {
        CHECK(count > 0);
        std::vector<uint32_t> const& variables = Y_expr_tree.get_variable_indices();
        ublas::vector<double> pdvalues = Y_expr_tree.pdvalue(means, variables);
        double result = 0;
        for (uint32_t i = 0; i < variables.size(); ++i) {
            for (uint32_t j = 0; j < variables.size(); ++j) {
                uint32_t idx1 = variables[i], idx2 = variables[j];
                result += pdvalues[i] * pdvalues[j] * cov_matrix(idx1, idx2) / count;
            }
        }
        if (is_std) {
            result = std::sqrt(result);
        }
        return result;
    }

    static double calc_delta_method_cov(ExprTree<double> const& X1_expr_tree, ExprTree<double> const& X2_expr_tree,
                                        size_t count, ublas::vector<double> const& means,
                                        ublas::matrix<double> const& cov_matrix) {
        const auto& X1_variables = X1_expr_tree.get_variable_indices();
        const auto& X2_variables = X2_expr_tree.get_variable_indices();
        std::vector<uint32_t> X1X2_variables;
        std::copy(X1_variables.begin(), X1_variables.end(), std::back_inserter(X1X2_variables));
        std::copy(X2_variables.begin(), X2_variables.end(), std::back_inserter(X1X2_variables));
        uint32_t X1_num_variables = X1_variables.size();
        uint32_t X2_num_variables = X2_variables.size();
        uint32_t X1X2_num_variables = X1_num_variables + X2_num_variables;

        ublas::matrix<double> cov_tmp(X1X2_num_variables, X1X2_num_variables);
        for (uint32_t i = 0; i < X1X2_num_variables; ++i) {
            for (uint32_t j = 0; j < X1X2_num_variables; ++j) {
                cov_tmp(i, j) = cov_matrix(X1X2_variables[i], X1X2_variables[j]);
            }
        }

        ublas::matrix<double> deriv_a_tmp(1, X1X2_num_variables, 0);
        ublas::matrix<double> deriv_b_tmp(X1X2_num_variables, 1, 0); // should be init with 0
        ublas::vector<double> pdvalues_a = X1_expr_tree.pdvalue(means, X1_variables);
        ublas::vector<double> pdvalues_b = X2_expr_tree.pdvalue(means, X2_variables);
        for (uint32_t i = 0; i < X1_num_variables; ++i) {
            deriv_a_tmp(0, i) = pdvalues_a(i);
        }
        for (uint32_t i = 0; i < X2_num_variables; ++i) {
            deriv_b_tmp(i + X1_num_variables, 0) = pdvalues_b(i);
        }
        return ublas::prod(ublas::matrix<double>(ublas::prod(deriv_a_tmp, cov_tmp)), deriv_b_tmp)(0, 0) / count;
    }

    ublas::matrix<double> XTX() const {
        ublas::matrix<double> ret(_num_variables, _num_variables);
        for (uint32_t i = 0; i < _num_variables; ++i) {
            for (uint32_t j = i; j < _num_variables; ++j) {
                ret(j, i) = ret(i, j) = _sum_xy(i, j);
            }
        }
        return ret;
    }

private:
    int _num_variables{-1};
    size_t _count{0};
    ublas::vector<double> _sum_x;
    ublas::triangular_matrix<double, ublas::upper> _sum_xy;
};

class DeltaMethodAggregateState {
public:
    DeltaMethodAggregateState() = default;
    DeltaMethodAggregateState(const DeltaMethodAggregateState&) = delete;
    DeltaMethodAggregateState(DeltaMethodAggregateState&&) = delete;

    bool operator==(DeltaMethodAggregateState const& other) const { return _params == other._params; }

    void check_params(DeltaMethodAggregateState const& other) const { DCHECK(_params == other._params); }

    DeltaMethodAggregateState(const uint8_t* serialized_data) { deserialize(serialized_data); }

    bool is_uninitialized() const { return _params.is_uninitialized(); }

    void reset() {
        _params.reset();
        _stats.reset();
    }

    double get_result() const {
        DCHECK(!is_uninitialized());
        if (_stats.count() == 0 || _stats.count() == 1) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        return DeltaMethodStats::calc_delta_method(ExprTree<double>(_params.expression(), _params.num_variables()),
                                                   _stats.count(), _stats.means(), _stats.cov_matrix(),
                                                   _params.is_stderr());
    }

    void update(const double* input, int num_variables) {
        CHECK(!is_uninitialized());
        _stats.update(input, num_variables);
    }

    bool init(std::string const& expression, bool is_std, int num_cols) {
        CHECK(num_cols >= 0);
        _params.init(num_cols, expression, is_std);
        _stats.init(num_cols);
        return true;
    }

    void merge(DeltaMethodAggregateState const& other) {
        if (other.is_uninitialized()) {
            reset();
            return;
        }
        CHECK(_params == other._params);
        _stats.merge(other._stats);
    }

    void serialize(uint8_t*& data) const {
        _params.serialize(data);
        if (_params.is_uninitialized()) {
            return;
        }
        _stats.serialize(data);
    }

    void deserialize(const uint8_t*& data) {
        _params.deserialize(data);
        if (_params.is_uninitialized()) {
            return;
        }
        _stats.init(_params.num_variables());
        _stats.deserialize(data);
    }

    size_t serialized_size() const {
        size_t size = _params.serialized_size();
        if (_params.is_uninitialized()) {
            return size;
        }
        return size + _stats.serialized_size();
    }

private:
    DeltaMethodParams _params;
    DeltaMethodStats _stats;
};

class DeltaMethodAggregateFunction
        : public AggregateFunctionBatchHelper<DeltaMethodAggregateState, DeltaMethodAggregateFunction> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {}

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const Column* data_col = columns[2];
        auto [input, array_size] =
                FunctionHelper::get_data_of_array<DeltaMethodDataElementColumnType, double>(data_col, row_num);
        if (input == nullptr) {
            ctx->set_error("Internal Error: fail to get data.");
            return;
        }

        if (this->data(state).is_uninitialized()) {
            const Column* expr_col = columns[0];
            const Column* is_std_col = columns[1];
            const auto* func_expr = down_cast<const DeltaMethodExprColumnType*>(expr_col);
            Slice expr_slice = func_expr->get_data()[0];
            std::string expression = expr_slice.to_string();
            const auto* type_column = FunctionHelper::unwrap_if_const<const DeltaMethodIsStdColumnType*>(is_std_col);
            if (type_column == nullptr) {
                ctx->set_error("Internal Error: fail to get is_std.");
                return;
            }
            bool is_std = type_column->get_data()[0];
            this->data(state).init(expression, is_std, array_size);
        }

        this->data(state).update(input, array_size);
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
        DeltaMethodAggregateState other(serialized_data);
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
        DCHECK(to->is_numeric() || to->is_decimal());
        double result;
        if (this->data(state).is_uninitialized()) {
            ctx->set_error("Internal Error: state not initialized.");
            return;
        }
        result = this->data(state).get_result();
        down_cast<DeltaMethodResultColumnType*>(to)->append(result);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {}

    std::string get_name() const override { return "delta_method"; }
};

} // namespace starrocks
