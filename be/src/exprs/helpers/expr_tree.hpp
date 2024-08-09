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

#include <fmt/format.h>

#include <boost/numeric/ublas/matrix.hpp>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace starrocks {

namespace ublas = boost::numeric::ublas;

template <typename T>
using ExprVector = ublas::vector<T>;

template <typename T>
class ExprNode;

template <typename T>
class ExprOperator {
public:
    [[maybe_unused]] virtual ~ExprOperator() = default;
    [[nodiscard]] virtual T value(std::unique_ptr<ExprNode<T>> const& lhs, std::unique_ptr<ExprNode<T>> const& rhs,
                                  ExprVector<T> const& input) const = 0;
    [[nodiscard]] virtual T pdvalue(std::unique_ptr<ExprNode<T>> const& lhs, std::unique_ptr<ExprNode<T>> const& rhs,
                                    ExprVector<T> const& input, uint32_t data_idx) const = 0;
    [[nodiscard]] virtual std::string dump() const = 0;
    static std::unordered_map<char, std::shared_ptr<ExprOperator<T>>> op_factory;
};

template <typename T>
class ExprPlusOperator : public ExprOperator<T> {
public:
    [[nodiscard]] T value(std::unique_ptr<ExprNode<T>> const& lhs, std::unique_ptr<ExprNode<T>> const& rhs,
                          ExprVector<T> const& input) const override {
        return lhs->value(input) + rhs->value(input);
    }
    [[nodiscard]] T pdvalue(std::unique_ptr<ExprNode<T>> const& lhs, std::unique_ptr<ExprNode<T>> const& rhs,
                            ExprVector<T> const& input, uint32_t data_idx) const override {
        return lhs->pdvalue(input, data_idx) + rhs->pdvalue(input, data_idx);
    }
    [[nodiscard]] std::string dump() const override { return "+"; }
};

template <typename T>
class ExprMinusOperator : public ExprOperator<T> {
public:
    [[nodiscard]] T value(std::unique_ptr<ExprNode<T>> const& lhs, std::unique_ptr<ExprNode<T>> const& rhs,
                          ExprVector<T> const& input) const override {
        return lhs->value(input) - rhs->value(input);
    }
    [[nodiscard]] T pdvalue(std::unique_ptr<ExprNode<T>> const& lhs, std::unique_ptr<ExprNode<T>> const& rhs,
                            ExprVector<T> const& input, uint32_t data_idx) const override {
        return lhs->pdvalue(input, data_idx) - rhs->pdvalue(input, data_idx);
    }
    [[nodiscard]] std::string dump() const override { return "-"; }
};

template <typename T>
class ExprMultiplyOperator : public ExprOperator<T> {
public:
    [[nodiscard]] T value(std::unique_ptr<ExprNode<T>> const& lhs, std::unique_ptr<ExprNode<T>> const& rhs,
                          ExprVector<T> const& input) const override {
        return lhs->value(input) * rhs->value(input);
    }
    [[nodiscard]] T pdvalue(std::unique_ptr<ExprNode<T>> const& lhs, std::unique_ptr<ExprNode<T>> const& rhs,
                            ExprVector<T> const& input, uint32_t data_idx) const override {
        return lhs->pdvalue(input, data_idx) * rhs->value(input) + lhs->value(input) * rhs->pdvalue(input, data_idx);
    }
    [[nodiscard]] std::string dump() const override { return "*"; }
};

template <typename T>
class ExprDivideOperator : public ExprOperator<T> {
public:
    [[nodiscard]] T value(std::unique_ptr<ExprNode<T>> const& lhs, std::unique_ptr<ExprNode<T>> const& rhs,
                          ExprVector<T> const& input) const override {
        return lhs->value(input) / rhs->value(input);
    }
    [[nodiscard]] T pdvalue(std::unique_ptr<ExprNode<T>> const& lhs, std::unique_ptr<ExprNode<T>> const& rhs,
                            ExprVector<T> const& input, uint32_t data_idx) const override {
        return (lhs->pdvalue(input, data_idx) * rhs->value(input) - rhs->pdvalue(input, data_idx) * lhs->value(input)) /
               (rhs->value(input) * rhs->value(input));
    }
    [[nodiscard]] std::string dump() const override { return "/"; }
};

template <typename T>
std::shared_ptr<ExprOperator<T>> get_expr_operator(char op) {
    return ExprOperator<T>::op_factory[op];
}

template <typename T>
class ExprNode {
public:
    virtual ~ExprNode() = default;
    [[nodiscard]] virtual T value(ExprVector<T> const& input) const = 0;
    [[nodiscard]] virtual T pdvalue(ExprVector<T> const& input, uint32_t data_idx) const = 0;
    [[nodiscard]] virtual std::string dump() const = 0;
};

template <typename T>
class ExprLeafNode : public ExprNode<T> {
public:
    union LeafData {
        uint32_t idx;
        T value;
    };

    ExprLeafNode(LeafData data, bool is_variable) : _data(data), _is_variable(is_variable) {}

    [[nodiscard]] T value(ExprVector<T> const& input) const override {
        if (_is_variable) {
            return input(_data.idx);
        }
        return _data.value;
    }

    [[nodiscard]] T pdvalue([[maybe_unused]] ExprVector<T> const& input, uint32_t data_idx) const override {
        if (_is_variable && _data.idx == data_idx) {
            return 1;
        }
        return 0;
    }

    [[nodiscard]] std::string dump() const override {
        if (_is_variable) {
            return std::string("x") + std::to_string(_data.idx + 1);
        }
        std::stringstream ss;
        ss << _data.value;
        return ss.str();
    }

private:
    LeafData _data{};
    bool _is_variable;
};

template <typename T>
class ExprInternalNode : public ExprNode<T> {
public:
    explicit ExprInternalNode(std::unique_ptr<ExprNode<T>> left, std::shared_ptr<ExprOperator<T>> op = {},
                              std::unique_ptr<ExprNode<T>> rhs = {})
            : _lhs(std::move(left)), _op(std::move(op)), _rhs(std::move(rhs)) {}

    [[nodiscard]] T value(ExprVector<T> const& input) const override { return _op->value(_lhs, _rhs, input); }

    [[nodiscard]] T pdvalue(ExprVector<T> const& input, uint32_t data_idx) const override {
        return _op->pdvalue(_lhs, _rhs, input, data_idx);
    }

    [[nodiscard]] std::string dump() const override {
        return "(" + _lhs->dump() + " " + _op->dump() + " " + _rhs->dump() + ")";
    }

private:
    std::unique_ptr<ExprNode<T>> _lhs{};
    std::shared_ptr<ExprOperator<T>> _op{};
    std::unique_ptr<ExprNode<T>> _rhs{};
};

template <typename T>
class ExprTree {
public:
    ExprTree() = default;
    ExprTree(const std::string& expr, uint32_t num_variables) { CHECK(init(expr, num_variables)); }
    ExprTree(ExprTree const&) = delete;
    ExprTree(ExprTree&& other) noexcept
            : _node(std::move(other._node)), _variable_indices(std::move(other._variable_indices)) {}
    ExprTree& operator=(ExprTree&& other) noexcept {
        ExprTree(std::move(other)).swap(*this);
        return *this;
    }
    void swap(ExprTree& other) { std::swap(_node, other._node); }

    bool init(const std::string& expr, std::unordered_map<std::string, uint32_t> const& symbol2idx) {
        reset();
        return _parse(expr, symbol2idx);
    }

    bool init(const std::string& expr, uint32_t num_variables) {
        reset();
        std::unordered_map<std::string, uint32_t> symbol2idx;
        for (int i = 1; i <= num_variables; ++i) {
            symbol2idx[fmt::format("x{}", i)] = i - 1;
        }
        return _parse(expr, symbol2idx);
    }

    [[nodiscard]] T value(ExprVector<T> const& input) const { return _node->value(input); }

    [[nodiscard]] ExprVector<T> pdvalue(ExprVector<T> const& input) const {
        ExprVector<T> result(input.size());
        for (uint32_t i = 0; i < input.size(); ++i) {
            result(i) = _node->pdvalue(input, i);
        }
        return result;
    }

    [[nodiscard]] ExprVector<T> pdvalue(ExprVector<T> const& input, std::vector<uint32_t> const& indices) const {
        ExprVector<T> result(indices.size());
        for (uint32_t i = 0; i < indices.size(); ++i) {
            result(i) = _node->pdvalue(input, indices[i]);
        }
        return result;
    }

    [[nodiscard]] std::string dump() const { return _node->dump(); }

    bool is_init() const { return _node == nullptr; }

    void reset() {
        _node = nullptr;
        std::vector<uint32_t>().swap(_variable_indices);
    }

    std::vector<uint32_t> const& get_variable_indices() const { return _variable_indices; }

private:
    std::unique_ptr<ExprNode<T>> _node;
    std::vector<uint32_t> _variable_indices;
    const static std::unordered_map<char, int> op2rk;

    bool _parse(const std::string& expr, std::unordered_map<std::string, uint32_t> const& symbol2idx);

    bool _is_operator(char ch) { return op2rk.count(ch) != 0; }

    bool _calc_one_operator(char op, std::vector<std::unique_ptr<ExprNode<T>>>& data_stack) {
        if (data_stack.empty()) {
            return false;
        }
        std::unique_ptr<ExprNode<T>> r_expr = std::move(data_stack.back());
        data_stack.pop_back();
        if (data_stack.empty()) {
            return false;
        }
        std::unique_ptr<ExprNode<T>> l_expr = std::move(data_stack.back());
        data_stack.pop_back();
        data_stack.emplace_back(
                std::make_unique<ExprInternalNode<T>>(std::move(l_expr), get_expr_operator<T>(op), std::move(r_expr)));
        return true;
    }

    bool _match_left_bracket(std::vector<char>& op_stack, std::vector<std::unique_ptr<ExprNode<T>>>& data_stack) {
        while (true) {
            if (op_stack.empty()) {
                return false;
            }
            auto last_op = op_stack.back();
            op_stack.pop_back();
            if (last_op == '(') {
                return true;
            }
            if (!_calc_one_operator(last_op, data_stack)) {
                return false;
            }
        }
    }

    bool _calc_higher_operators(char op_now, std::vector<char>& op_stack,
                                std::vector<std::unique_ptr<ExprNode<T>>>& data_stack) {
        while (!op_stack.empty() && op2rk.at(op_now) <= op2rk.at(op_stack.back())) {
            auto last_op = op_stack.back();
            op_stack.pop_back();
            if (!_calc_one_operator(last_op, data_stack)) {
                return false;
            }
        }
        return true;
    }

    std::pair<bool, T> _try_parse_pure_number(std::string const& expr) {
        char* err_ptr = nullptr;
        auto value = strtod(expr.c_str(), &err_ptr);
        if (err_ptr != expr.c_str() + expr.length()) {
            return {false, {}};
        }
        return {true, value};
    }
};

// TODO(cooperxiong): add more operators
template <typename T>
std::unordered_map<char, std::shared_ptr<ExprOperator<T>>> ExprOperator<T>::op_factory{
        {'+', std::make_shared<ExprPlusOperator<T>>()},
        {'-', std::make_shared<ExprMinusOperator<T>>()},
        {'*', std::make_shared<ExprMultiplyOperator<T>>()},
        {'/', std::make_shared<ExprDivideOperator<T>>()}};

template <typename T>
const std::unordered_map<char, int> ExprTree<T>::op2rk{
        {')', -1}, {'(', 0}, {'+', 1}, {'-', 1}, {'*', 2}, {'/', 2},
};

template <typename T>
bool ExprTree<T>::_parse(const std::string& input_expr, std::unordered_map<std::string, uint32_t> const& symbol2idx) {
    std::vector<char> op_stack;
    std::vector<std::unique_ptr<ExprNode<T>>> data_stack;
    std::string expr;
    for (auto ch : '(' + input_expr + ')') {
        if (ch != ' ') {
            expr += ch;
        }
    }
    uint32_t length = expr.length();
    for (uint32_t l = 0, r; l < length; l = r) {
        r = l + 1;
        if (expr[l] == '(') {
            op_stack.emplace_back('(');
            continue;
        }
        if (expr[l] == ')') {
            if (!_match_left_bracket(op_stack, data_stack)) {
                return false;
            }
            continue;
        }
        if (_is_operator(expr[l])) {
            if ((expr[l] == '+' || expr[l] == '-') && (l == 0 || expr[l - 1] == '(')) {
                data_stack.emplace_back(
                        std::make_unique<ExprLeafNode<T>>(typename ExprLeafNode<T>::LeafData{.value = 0}, false));
            }
            auto op_now = expr[l];
            if (!_calc_higher_operators(op_now, op_stack, data_stack)) {
                return false;
            }
            op_stack.emplace_back(op_now);
            continue;
        }
        while (r < length && !_is_operator(expr[r])) {
            r += 1;
        }
        auto symbol = expr.substr(l, r - l);
        auto [is_pure_number, pure_number_value] = _try_parse_pure_number(symbol);
        if (is_pure_number) {
            data_stack.emplace_back(std::make_unique<ExprLeafNode<T>>(
                    typename ExprLeafNode<T>::LeafData{.value = pure_number_value}, false));
            continue;
        }
        if (!symbol2idx.count(symbol)) {
            return false;
        }
        uint32_t _variable_index = symbol2idx.at(symbol);
        data_stack.emplace_back(
                std::make_unique<ExprLeafNode<T>>(typename ExprLeafNode<T>::LeafData{.idx = _variable_index}, true));
        _variable_indices.emplace_back(_variable_index);
    }
    if (data_stack.size() != 1 || !op_stack.empty()) {
        return false;
    }
    _node = std::move(data_stack[0]);
    std::sort(_variable_indices.begin(), _variable_indices.end());
    _variable_indices.erase(std::unique(_variable_indices.begin(), _variable_indices.end()), _variable_indices.end());
    return true;
}

} // namespace starrocks
