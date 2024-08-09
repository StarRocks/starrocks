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
#include <velocypack/Builder.h>
#include <velocypack/Iterator.h>
#include <velocypack/Value.h>

#include <algorithm>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/math/distributions/fisher_f.hpp>
#include <boost/math/distributions/students_t.hpp>
#include <boost/numeric/ublas/matrix.hpp>
#include <boost/numeric/ublas/matrix_expression.hpp>
#include <cstddef>

#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/helpers/serialize_helpers.hpp"
#include "exprs/function_helper.h"
#include "exprs/helpers/serialize_helpers.hpp"
#include "gutil/casts.h"
#include "storage/types.h"
#include "types/logical_type.h"
#include "util/json.h"

namespace starrocks {

using OlsYColumnType = RunTimeColumnType<TYPE_DOUBLE>;
using OlsXArrayColumnType = RunTimeColumnType<TYPE_ARRAY>;
using OlsXColumnType = RunTimeColumnType<TYPE_DOUBLE>;
using OlsUseBiasColumnType = RunTimeColumnType<TYPE_BOOLEAN>;
using OlsModelColumnType = RunTimeColumnType<TYPE_JSON>;
using OlsJsonKeyColumnType = RunTimeColumnType<TYPE_VARCHAR>;
using OlsModelArrayColumnType = RunTimeColumnType<TYPE_ARRAY>;
using OlsResultColumnType = RunTimeColumnType<TYPE_VARCHAR>;
using OlsWeightColumnType = RunTimeColumnType<TYPE_DOUBLE>;
using OlsArgNamesColumnType = RunTimeColumnType<TYPE_VARCHAR>;
using OlsMatrixColumnType = RunTimeColumnType<TYPE_JSON>;

class OlsParams {
public:
    bool operator==(const OlsParams& other) const {
        return _use_bias == other._use_bias && _num_variables == other._num_variables;
    }

    bool is_uninitialized() const { return _num_variables == -1; }

    void reset() {
        _num_variables = -1;
        _use_bias = false;
    }

    void init(int num_variables, bool use_bias, std::optional<std::string> const& arg_names,
              std::optional<JsonValue> const& XTX, std::optional<JsonValue> const& XTX_inv) {
        _num_variables = num_variables;
        _use_bias = use_bias;
        if (arg_names.has_value()) {
            _arg_names = arg_names.value();
        }
        if (XTX.has_value() && XTX_inv.has_value()) {
            _XTX = std::move(XTX.value());
            _XTX_inv = std::move(XTX_inv.value());
        }
    }

    void serialize(uint8_t*& data) const {
        SerializeHelpers::serialize(_num_variables, data);
        if (is_uninitialized()) {
            return;
        }
        uint8_t use_bias = _use_bias;
        SerializeHelpers::serialize(&use_bias, data);
        uint32_t length = _arg_names.length();
        SerializeHelpers::serialize(&length, data);
        SerializeHelpers::serialize(_arg_names.data(), data, length);
        Slice XTX_slice = _XTX.get_slice();
        length = XTX_slice.size;
        SerializeHelpers::serialize(&length, data);
        SerializeHelpers::serialize(XTX_slice.data, data, length);
        Slice XTX_inv_slice = _XTX_inv.get_slice();
        length = XTX_inv_slice.size;
        SerializeHelpers::serialize(&length, data);
        SerializeHelpers::serialize(XTX_inv_slice.data, data, length);
    }

    void deserialize(const uint8_t*& data) {
        SerializeHelpers::deserialize(data, &_num_variables);
        if (is_uninitialized()) {
            return;
        }
        uint8_t use_bias;
        SerializeHelpers::deserialize(data, &use_bias);
        _use_bias = use_bias;
        uint32_t length = 0;
        SerializeHelpers::deserialize(data, &length);
        _arg_names.resize(length);
        SerializeHelpers::deserialize(data, _arg_names.data(), length);
        SerializeHelpers::deserialize(data, &length);
        std::string XTX_str(length, ' ');
        SerializeHelpers::deserialize(data, XTX_str.data(), length);
        _XTX = JsonValue(std::move(XTX_str));
        SerializeHelpers::deserialize(data, &length);
        std::string XTX_inv_str(length, ' ');
        SerializeHelpers::deserialize(data, XTX_inv_str.data(), length);
        _XTX_inv = JsonValue(std::move(XTX_inv_str));
    }

    size_t serialized_size() const {
        if (is_uninitialized()) {
            return sizeof(_num_variables);
        }
        return sizeof(_num_variables) + sizeof(uint8_t) + sizeof(uint32_t) * 3 + _arg_names.size() +
               _XTX.get_slice().size + _XTX_inv.get_slice().size;
    }

    int num_variables() const { return _num_variables; }

    bool use_bias() const { return _use_bias; }

    std::string const& arg_names() const { return _arg_names; }

    JsonValue const& XTX() const { return _XTX; }

    JsonValue const& XTX_inv() const { return _XTX_inv; }

private:
    bool _use_bias{false};
    int32_t _num_variables{-1};
    std::string _arg_names;
    JsonValue _XTX;
    JsonValue _XTX_inv;
};

class OlsBaseStats {
public:
    OlsBaseStats() = default;
    OlsBaseStats(const OlsBaseStats&) = delete;
    OlsBaseStats(OlsBaseStats&&) = delete;

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

    void merge(OlsBaseStats const& other) {
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

class OlsStats {
public:
    OlsStats() = default;
    OlsStats(const OlsStats&) = delete;
    OlsStats(OlsStats&&) = delete;

    bool is_uninitialized() const { return _stats.is_uninitialized(); }

    void init(int num_variables) {
        CHECK(num_variables >= 0);
        _stats.init(num_variables);
        _XTy = ublas::matrix<double>(num_variables, 1, 0);
        _y_stats.init(1);
    }

    void reset() { _stats.reset(); }

    void update(double y, const double* X, int length) {
        CHECK_EQ(_stats.num_variables(), length);
        _stats.update(X, length);
        for (uint32_t i = 0; i < length; ++i) {
            _XTy(i, 0) += y * X[i];
        }
        _y_stats.update(&y, 1);
    }

    void merge(OlsStats const& other) {
        CHECK_EQ(_stats.num_variables(), other._stats.num_variables());
        _stats.merge(other._stats);
        for (uint32_t i = 0; i < num_variables(); ++i) {
            _XTy(i, 0) += other._XTy(i, 0);
        }
        _y_stats.merge(other._y_stats);
    }

    int num_variables() const { return _stats.num_variables(); }

    void serialize(uint8_t*& data) const {
        CHECK(!is_uninitialized());
        _stats.serialize(data);
        SerializeHelpers::serialize(_XTy.data().begin(), data, num_variables());
        _y_stats.serialize(data);
    }

    void deserialize(const uint8_t*& data) {
        CHECK(!is_uninitialized());
        _stats.deserialize(data);
        SerializeHelpers::deserialize(data, _XTy.data().begin(), num_variables());
        _y_stats.deserialize(data);
    }

    size_t serialized_size() const {
        CHECK(!is_uninitialized());
        size_t size = _stats.serialized_size();
        size += sizeof(double) * num_variables();
        size += _y_stats.serialized_size();
        return size;
    }

    ublas::matrix<double> XTX() const { return _stats.XTX(); }

    ublas::matrix<double> XTy() const { return _XTy; }

    size_t count() const { return _stats.count(); }

    ublas::matrix<double> cov_matrix() const { return _stats.cov_matrix(); }

    double var_y() const { return _y_stats.cov_matrix()(0, 0); }

    double sum_y_square() const { return _y_stats.XTX()(0, 0); }

private:
    OlsBaseStats _stats;
    OlsBaseStats _y_stats;
    ublas::matrix<double> _XTy;
};

class OlsState {
public:
    struct StatsResult {
        ublas::matrix<double> coef;
        ublas::matrix<double> std;
        ublas::matrix<double> t_stat;
        ublas::matrix<double> p_value;
        size_t biased_size;
        size_t n;
        size_t k;
        size_t df;
        double sigma; // sum of (y_hat - y)^2
        double standard_error;
        double rows_required;
        double var_y;
        double adjusted_rows_required;
        double f_statistic;
        double f_value;
    };

    OlsState() = default;
    OlsState(const OlsState&) = delete;
    OlsState(OlsState&&) = delete;

    bool operator==(OlsState const& other) const { return _params == other._params; }

    void check_params(OlsState const& other) const { DCHECK(_params == other._params); }

    OlsState(const uint8_t* serialized_data) { deserialize(serialized_data); }

    bool is_uninitialized() const { return _params.is_uninitialized(); }

    void reset() {
        _params.reset();
        _stats.reset();
    }

    void update(double y, const double* X, int num_variables) {
        CHECK(!is_uninitialized());
        _stats.update(y, X, num_variables + use_bias());
    }

    void init(int32_t num_variables, std::optional<bool> use_bias, std::optional<std::string> const& arg_names,
              std::optional<JsonValue> const& XTX, std::optional<JsonValue> const& XTX_inv) {
        CHECK(num_variables >= 0);
        if (!use_bias.has_value()) {
            use_bias = false;
        }
        _params.init(num_variables, use_bias.value(), arg_names, XTX, XTX_inv);
        _stats.init(num_variables + use_bias.value());
    }

    void merge(OlsState const& other) {
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
        _stats.init(_params.num_variables() + use_bias());
        _stats.deserialize(data);
    }

    size_t serialized_size() const {
        size_t size = _params.serialized_size();
        if (_params.is_uninitialized()) {
            return size;
        }
        return size + _stats.serialized_size();
    }

    bool use_bias() const { return _params.use_bias(); }

    void build_model(vpack::Builder& builder) const {
        vpack::ObjectBuilder obj_builder(&builder);

        StatsResult result;
        auto status = calc_stats_result(result);
        if (status.has_value()) {
            builder.add("Error", vpack::Value(status.value()));
            return;
        }

        ublas::matrix<double> const& coef = result.coef;
        obj_builder->add("name", vpack::Value("ols"));
        obj_builder->add("num_variables", vpack::Value(_params.num_variables()));
        obj_builder->add("use_bias", vpack::Value(use_bias()));
        {
            vpack::ArrayBuilder coef_builder(&builder, "coef");
            for (uint32_t i = 0; i < result.biased_size; ++i) {
                coef_builder->add(vpack::Value(coef(i, 0)));
            }
        }
    }

    std::optional<std::string> calc_stats_result(StatsResult& result) const {
        if (_stats.count() < 2) {
            return fmt::format("Given too less data, num_rows({}) is less than 2.", _stats.count());
        }
        ublas::matrix<double> XTX = _stats.XTX();
        ublas::matrix<double> XTy = _stats.XTy();
        ublas::matrix<double> XTX_inv(XTX.size1(), XTX.size2(), 0);
        std::vector<size_t> nan_indices;
        if (!MathHelpers::invert_matrix(XTX, XTX_inv, nan_indices)) {
            return fmt::format("Unable to invert matrix XTX({})", MathHelpers::debug_matrix(XTX));
        }

        for (const auto& nan_index : nan_indices) {
            for (size_t i = 0; i < XTX_inv.size1(); i++) {
                XTX_inv(i, nan_index) = XTX_inv(nan_index, i) = 0;
                XTX(nan_index, i) = XTX(i, nan_index) = 0;
                XTy(nan_index, 0) = 0;
            }
        }

        ublas::matrix<double> coef = ublas::prod(XTX_inv, XTy);

        size_t biased_size = _params.num_variables() + _params.use_bias();

        size_t n = _stats.count();
        size_t k = _params.num_variables();
        if (n <= k + 1) {
            // df, aka n - k - 1, should be greater than 0
            return fmt::format("Given too less data, df: n({})-k({})-1 is less than one.", n, k);
        }
        size_t df = n - k - 1;
        double sigma = (_stats.sum_y_square() - 2 * prod(ublas::trans(coef), XTy)(0, 0) +
                        prod(ublas::matrix<double>(prod(ublas::trans(coef), XTX)), coef)(0, 0)) /
                       df;
        double standard_error = std::sqrt(sigma);

        ublas::matrix<double> cov_xx = _stats.cov_matrix();
        ublas::matrix<double> var_x(biased_size, biased_size, 0);
        for (uint32_t i = 0; i < _params.num_variables(); ++i) {
            for (uint32_t j = i; j < _params.num_variables(); ++j) {
                var_x(i, j) = var_x(j, i) = cov_xx(i, j);
            }
        }
        ublas::matrix<double> var_predict_y = prod(ublas::trans(coef), ublas::matrix<double>(prod(var_x, coef)));

        ublas::matrix<double> diag_XTX_inv(1, biased_size);
        for (size_t i = 0; i < biased_size; ++i) {
            diag_XTX_inv(0, i) = XTX_inv(i, i);
        }

        ublas::matrix<double> std(1, biased_size);
        if (_params.XTX().get_slice().empty() || _params.XTX_inv().get_slice().empty()) {
            std = diag_XTX_inv * sigma;
        } else {
            auto xx_weighted_or_status = json2matrix(_params.XTX());
            if (!xx_weighted_or_status.ok()) {
                return xx_weighted_or_status.status().to_string();
            }
            ublas::matrix<double> xx_weighted = xx_weighted_or_status.value();
            auto xx_weighted_inv_or_status = json2matrix(_params.XTX_inv());
            if (!xx_weighted_inv_or_status.ok()) {
                return xx_weighted_inv_or_status.status().to_string();
            }
            ublas::matrix<double> xx_weighted_inv = xx_weighted_inv_or_status.value();

            if (!(xx_weighted_inv.size1() == xx_weighted_inv.size2() && xx_weighted_inv.size1() == biased_size &&
                  xx_weighted.size1() == xx_weighted.size2() && xx_weighted.size1() == biased_size)) {
                return fmt::format(
                        "xx_weighted_inv.size1({}) == xx_weighted_inv.size2({}) == xx_weighted.size1({}) == "
                        "xx_weighted.size2({}) == biased_size({}): check failed.",
                        xx_weighted_inv.size1(), xx_weighted_inv.size2(), xx_weighted.size1(), xx_weighted.size2(),
                        biased_size);
            }
            ublas::matrix<double> tmp =
                    prod(static_cast<ublas::matrix<double>>(prod(xx_weighted_inv, xx_weighted)), xx_weighted_inv);
            for (size_t i = 0; i < biased_size; ++i) {
                std(0, i) = tmp(i, i);
            }
        }

        std::transform(std.data().begin(), std.data().end(), std.data().begin(), [](double x) { return std::sqrt(x); });

        double var_y = _stats.var_y();

        ublas::matrix<double> t_stat(1, biased_size);
        for (size_t i = 0; i < coef.size1(); ++i) {
            t_stat(0, i) = coef(i, 0) / std(0, i);
        }

        ublas::matrix<double> p_value(1, biased_size);
        auto student = boost::math::students_t_distribution<double>(df);
        for (size_t i = 0; i < biased_size; ++i) {
            double t_stat_elem = t_stat(0, i);
            if (std::isnan(t_stat_elem)) {
                p_value(0, i) = t_stat_elem;
            } else if (std::isinf(t_stat_elem)) {
                p_value(0, i) = 0;
            } else {
                p_value(0, i) = 2 * cdf(complement(student, std::abs(t_stat(0, i))));
            }
        }

        double rows_required = var_predict_y(0, 0) / var_y;
        double adjusted_rows_required = 1 - (1 - rows_required) * (n - 1) / df;

        double sse = var_y - var_predict_y(0, 0);
        double f_statistic = (var_predict_y(0, 0) / k) / (sse / df);
        double f_value = 1;
        if (f_statistic > 0) {
            if (std::isnan(f_statistic)) {
                f_value = f_statistic;
            } else if (std::isinf(f_statistic)) {
                f_value = 0;
            } else {
                f_value = cdf(complement(boost::math::fisher_f(k, df), f_statistic));
            }
        }

        result.coef = std::move(coef);
        result.std = std::move(std);
        result.t_stat = std::move(t_stat);
        result.p_value = std::move(p_value);
        result.var_y = var_y;
        result.n = n;
        result.k = k;
        result.df = df;
        result.sigma = sigma;
        result.standard_error = standard_error;
        result.rows_required = rows_required;
        result.adjusted_rows_required = adjusted_rows_required;
        result.f_statistic = f_statistic;
        result.f_value = f_value;
        result.biased_size = biased_size;

        return std::nullopt;
    }

    std::string get_result() const {
        StatsResult stats_result;
        auto status = calc_stats_result(stats_result);
        if (status.has_value()) {
            return status.value();
        }

        std::string result;
        result += "\nCall:\n  lm( formula = y ~";
        std::vector<std::string> argument_names;
        boost::split(argument_names, _params.arg_names(), boost::is_any_of(","));
        for (auto& name : argument_names) {
            boost::trim(name);
        }
        argument_names.erase(std::remove_if(argument_names.begin(), argument_names.end(),
                                            [](std::string const& str) { return str.empty(); }),
                             argument_names.end());
        if (argument_names.empty()) {
            argument_names = std::vector<std::string>{"y"};
            for (size_t i = 1; i <= _params.num_variables(); ++i) {
                argument_names.emplace_back(fmt::format("x{}", i));
            }
        } else if (argument_names.size() != _params.num_variables() + 1) {
            return fmt::format("Size of argument_names should be {}, but get {}", _params.num_variables() + 1,
                               argument_names.size());
        }
        for (size_t i = 1; i <= _params.num_variables(); ++i) {
            auto const& argument_name = argument_names[i];
            if (i > 1) {
                result += "+";
            }
            result += " " + argument_name + " ";
        }

        result += ")\n\n";
        result += "Coefficients:\n";
        result += MathHelpers::to_string_with_precision(".", 16) + MathHelpers::to_string_with_precision("Estimate") +
                  MathHelpers::to_string_with_precision("Std. Error") +
                  MathHelpers::to_string_with_precision("t value") + MathHelpers::to_string_with_precision("Pr(>|t|)") +
                  "\n";
        if (_params.use_bias()) {
            result += MathHelpers::to_string_with_precision("(Intercept)", 16) +
                      MathHelpers::to_string_with_precision(stats_result.coef(stats_result.biased_size - 1, 0)) +
                      MathHelpers::to_string_with_precision(stats_result.std(0, stats_result.biased_size - 1)) +
                      MathHelpers::to_string_with_precision(stats_result.t_stat(0, stats_result.biased_size - 1)) +
                      MathHelpers::to_string_with_precision(stats_result.p_value(0, stats_result.biased_size - 1)) +
                      "\n";
        }

        for (size_t i = 0; i < _params.num_variables(); ++i) {
            std::string argument_name = argument_names[i + 1];
            result += MathHelpers::to_string_with_precision(argument_name, 16) +
                      MathHelpers::to_string_with_precision(stats_result.coef(i, 0)) +
                      MathHelpers::to_string_with_precision(stats_result.std(0, i)) +
                      MathHelpers::to_string_with_precision(stats_result.t_stat(0, i)) +
                      MathHelpers::to_string_with_precision(stats_result.p_value(0, i)) + "\n";
        }
        result += "\nResidual standard error: " + std::to_string(stats_result.standard_error) + " on " +
                  std::to_string(stats_result.df) + " degrees of freedom\n";
        result += "Multiple R-squared: " + std::to_string(stats_result.rows_required) +
                  ", Adjusted R-squared: " + std::to_string(stats_result.adjusted_rows_required) + "\n";
        result += "F-statistic: " + std::to_string(stats_result.f_statistic) + " on " + std::to_string(stats_result.k) +
                  " and " + std::to_string(stats_result.df) + " DF,  p-value: " + std::to_string(stats_result.f_value) +
                  "\n";
        return result;
    }

private:
    OlsParams _params;
    OlsStats _stats;

    static StatusOr<ublas::matrix<double>> json2matrix(JsonValue const& json) {
        std::vector<std::vector<double>> matrix;
        size_t num_cols = 0;
        for (const auto& row_elem : vpack::ArrayIterator(json.to_vslice())) {
            matrix.emplace_back();
            JsonValue row(row_elem);
            for (auto const& col_elem : vpack::ArrayIterator(row.to_vslice())) {
                JsonValue col(col_elem);
                ASSIGN_OR_RETURN(auto col_value, col.get_double());
                matrix.back().emplace_back(col_value);
            }
            if (num_cols == 0) {
                num_cols = matrix.back().size();
            } else if (matrix.back().size() != num_cols) {
                return Status::InvalidArgument("invalid matrix");
            }
        }
        size_t num_rows = matrix.size();
        ublas::matrix<double> ret(num_rows, num_cols);
        for (int i = 0; i < num_rows; ++i) {
            for (int j = 0; j < num_cols; ++j) {
                ret(i, j) = matrix[i][j];
            }
        }
        return ret;
    }
};

template <bool return_model, bool use_weights>
class OlsAggregateFunction
        : public AggregateFunctionBatchHelper<OlsState, OlsAggregateFunction<return_model, use_weights>> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {}

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        // y, X, weight(?), use_bias, arg_names, XTX, XTX_inv
        const Column* data_col = columns[1];
        auto [input, array_size] = FunctionHelper::get_data_of_array<OlsXColumnType, double>(data_col, row_num);
        if (input == nullptr) {
            ctx->set_error("Internal Error: fail to get X.");
            return;
        }

        if (this->data(state).is_uninitialized()) {
            const Column* use_bias_col = columns[2 + use_weights];
            std::optional<bool> use_bias_opt;
            bool use_bias;
            if (FunctionHelper::get_data_of_column<OlsUseBiasColumnType>(use_bias_col, 0, use_bias)) {
                use_bias_opt = use_bias;
            }

            std::optional<std::string> arg_names_opt;
            if (ctx->get_num_args() > 3 + use_weights) {
                const auto* arg_names_col =
                        FunctionHelper::unwrap_if_const<const OlsArgNamesColumnType*>(columns[3 + use_weights]);
                if (arg_names_col == nullptr) {
                    ctx->set_error("Internal Error: fail to get `arg_names`.");
                    return;
                }
                arg_names_opt = arg_names_col->get_data()[0].to_string();
            }

            std::optional<JsonValue> XTX_inv_opt;
            if (ctx->get_num_args() > 4 + use_weights) {
                JsonValue* value;
                if (!FunctionHelper::get_data_of_column<OlsMatrixColumnType>(columns[4 + use_weights], 0, value)) {
                    ctx->set_error("Internal Error: fail to get `XTX_inv`.");
                    return;
                }
                XTX_inv_opt = *value;
            }

            std::optional<JsonValue> XTX_opt;
            if (ctx->get_num_args() > 5 + use_weights) {
                JsonValue* value;
                if (!FunctionHelper::get_data_of_column<OlsMatrixColumnType>(columns[5 + use_weights], 0, value)) {
                    ctx->set_error("Internal Error: fail to get `XTX`.");
                    return;
                }
                XTX_opt = *value;
            }

            this->data(state).init(array_size, use_bias_opt, arg_names_opt, XTX_opt, XTX_inv_opt);
        }

        const Column* y_col = columns[0];
        double y = 0;
        if (!FunctionHelper::get_data_of_column<OlsYColumnType>(y_col, row_num, y)) {
            LOG(WARNING) << "ols: fail to get y.";
            ctx->set_error("Internal Error: fail to get y");
            return;
        }

        std::vector<double> x(input, input + array_size);
        if (this->data(state).use_bias()) {
            x.emplace_back(1);
        }

        if constexpr (use_weights) {
            const Column* weight_col = columns[2];
            double weight = 0;
            if (!FunctionHelper::get_data_of_column<OlsWeightColumnType>(weight_col, row_num, weight)) {
                ctx->set_error("Internal Error: fail to get weight.");
                return;
            }
            if (weight < 0) {
                ctx->set_error(
                        fmt::format("Internal Error: weight should be no less than zero, but get {}.", weight).c_str());
                return;
            }
            weight = std::sqrt(weight);
            std::transform(x.begin(), x.end(), x.begin(), [weight](double t) { return t * weight; });
            y *= weight;
        }
        this->data(state).update(y, x.data(), array_size);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        column = FunctionHelper::unwrap_if_nullable<const Column*>(column, row_num);
        if (column == nullptr) {
            this->data(state).reset();
            ctx->set_error("Internal Error: fail to get intermidiate column");
            return;
        }
        DCHECK(column->is_binary());
        const uint8_t* serialized_data = reinterpret_cast<const uint8_t*>(column->get(row_num).get_slice().data);
        if (this->data(state).is_uninitialized()) {
            this->data(state).deserialize(serialized_data);
            return;
        }
        OlsState other(serialized_data);
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

        if constexpr (return_model) {
            vpack::Builder result_builder;
            this->data(state).build_model(result_builder);
            auto slice = result_builder.slice();
            JsonValue result_json(slice);
            down_cast<OlsModelColumnType&>(*to).append(std::move(result_json));
        } else {
            auto result_string = this->data(state).get_result();
            down_cast<OlsResultColumnType&>(*to).append(std::move(result_string));
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {}

    std::string get_name() const override {
        if constexpr (use_weights) {
            if constexpr (return_model) {
                return std::string(AllInSqlFunctions::wls_train);
            }
            return std::string(AllInSqlFunctions::wls);
        }
        if constexpr (return_model) {
            return std::string(AllInSqlFunctions::ols_train);
        }
        return std::string(AllInSqlFunctions::ols);
    }
};

} // namespace starrocks
