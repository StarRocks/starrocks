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

#include "exprs/like_predicate.h"

#include <memory>

#include "exprs/binary_function.h"
#include "glog/logging.h"
#include "gutil/strings/substitute.h"
#include "runtime/Volnitsky.h"
#include "util/defer_op.h"

namespace starrocks {

// A regex to match any regex pattern is equivalent to a substring search.
static const RE2 SUBSTRING_RE(R"((?:\.\*)*([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)(?:\.\*)*)", re2::RE2::Quiet);

// A regex to match any regex pattern which is equivalent to matching a constant string
// at the end of the string values.
static const RE2 ENDS_WITH_RE(R"((?:\.\*)*([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)\$)", re2::RE2::Quiet);

// A regex to match any regex pattern which is equivalent to matching a constant string
// at the end of the string values.
static const RE2 STARTS_WITH_RE(R"(\^([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)(?:\.\*)*)", re2::RE2::Quiet);

// A regex to match any regex pattern which is equivalent to a constant string match.
static const RE2 EQUALS_RE(R"(\^([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)\$)", re2::RE2::Quiet);

static const re2::RE2 LIKE_SUBSTRING_RE(R"((?:%+)(((\\%)|(\\_)|([^%_]))+)(?:%+))", re2::RE2::Quiet);
static const re2::RE2 LIKE_ENDS_WITH_RE(R"((?:%+)(((\\%)|(\\_)|([^%_]))+))", re2::RE2::Quiet);
static const re2::RE2 LIKE_STARTS_WITH_RE(R"((((\\%)|(\\_)|([^%_]))+)(?:%+))", re2::RE2::Quiet);
static const re2::RE2 LIKE_EQUALS_RE(R"((((\\%)|(\\_)|([^%_]))+))", re2::RE2::Quiet);
static const char* PROMPT_INFO = " so we switch to use re2.";

bool LikePredicate::hs_compile_and_alloc_scratch(const std::string& pattern, LikePredicateState* state,
                                                 FunctionContext* context, const Slice& slice) {
    if (hs_compile(pattern.c_str(), HS_FLAG_ALLOWEMPTY | HS_FLAG_DOTALL | HS_FLAG_UTF8 | HS_FLAG_SINGLEMATCH,
                   HS_MODE_BLOCK, nullptr, &state->database, &state->compile_err) != HS_SUCCESS) {
        std::stringstream error;
        auto chopped_size = std::min<size_t>(slice.size, 64);
        auto ellipsis = (chopped_size < slice.size) ? "..." : "";
        error << "Invalid hyperscan expression: " << std::string(slice.data, chopped_size) << ellipsis << ": "
              << state->compile_err->message << PROMPT_INFO;
        LOG(WARNING) << error.str().c_str();
        hs_free_compile_error(state->compile_err);
        return false;
    }

    if (hs_alloc_scratch(state->database, &state->scratch) != HS_SUCCESS) {
        std::stringstream error;
        error << "ERROR: Unable to allocate scratch space," << PROMPT_INFO;
        LOG(WARNING) << error.str().c_str();
        hs_free_database(state->database);
        return false;
    }

    return true;
}

template <bool full_match>
Status LikePredicate::compile_with_hyperscan_or_re2(const std::string& pattern, LikePredicateState* state,
                                                    FunctionContext* context, const Slice& slice) {
    if (!hs_compile_and_alloc_scratch(pattern, state, context, slice)) {
        RE2::Options opts;
        opts.set_never_nl(false);
        opts.set_dot_nl(true);
        opts.set_log_errors(false);

        state->re2 = std::make_shared<re2::RE2>(pattern, opts);
        if (!state->re2->ok()) {
            std::stringstream error;
            error << "Invalid re2 expression: " << pattern;
            return Status::InvalidArgument(error.str());
        }

        if constexpr (full_match) {
            state->function = &like_fn_with_long_constant_pattern;
        } else {
            state->function = &regex_fn_with_long_constant_pattern;
        }
    }

    return Status::OK();
}

// when pattern is one of (EQUALS | SUBSTRING | STARTS_WITH | ENDS_WITH) or variable value
// we use Re2.
// when pattern is a constant value except ((EQUALS | SUBSTRING | STARTS_WITH | ENDS_WITH) variable value)
// we use hyperscan.

// like predicate
Status LikePredicate::like_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }

    // @todo: should replace to mem pool
    auto state = new LikePredicateState();
    state->function = &like_fn;

    context->set_function_state(scope, state);

    // go row regex
    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    auto column = context->get_constant_column(1);
    auto pattern = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    std::string pattern_str = pattern.to_string();
    std::string search_string;

    if (RE2::FullMatch(pattern_str, LIKE_ENDS_WITH_RE, &search_string)) {
        remove_escape_character(&search_string);
        state->set_search_string(search_string);
        state->function = &constant_ends_with_fn;
    } else if (RE2::FullMatch(pattern_str, LIKE_STARTS_WITH_RE, &search_string)) {
        remove_escape_character(&search_string);
        state->set_search_string(search_string);
        state->function = &constant_starts_with_fn;
    } else if (RE2::FullMatch(pattern_str, LIKE_EQUALS_RE, &search_string)) {
        remove_escape_character(&search_string);
        state->set_search_string(search_string);
        state->function = &constant_equals_fn;
    } else if (RE2::FullMatch(pattern_str, LIKE_SUBSTRING_RE, &search_string)) {
        remove_escape_character(&search_string);
        state->set_search_string(search_string);
        state->function = &constant_substring_fn;
    } else {
        auto re_pattern = LikePredicate::template convert_like_pattern<true>(context, pattern);
        RETURN_IF_ERROR(compile_with_hyperscan_or_re2<true>(re_pattern, state, context, pattern));
    }

    return Status::OK();
}

Status LikePredicate::like_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete state;
    }
    return Status::OK();
}

StatusOr<ColumnPtr> LikePredicate::like(FunctionContext* context, const starrocks::Columns& columns) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    return (state->function)(context, columns);
}

// regex predicate
Status LikePredicate::regex_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }

    // @todo: should replace to mem pool
    auto* state = new LikePredicateState();
    context->set_function_state(scope, state);

    state->function = &regex_fn;

    // go row regex
    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    auto column = context->get_constant_column(1);
    auto pattern = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    std::string pattern_str = pattern.to_string();
    std::string search_string;

    // The following four conditionals check if the pattern is a constant string,
    // starts with a constant string and is followed by any number of wildcard characters,
    // ends with a constant string and is preceded by any number of wildcard characters or
    // has a constant substring surrounded on both sides by any number of wildcard
    // characters. In any of these conditions, we can search for the pattern more
    // efficiently by using our own string match functions rather than regex matching.
    if (RE2::FullMatch(pattern_str, EQUALS_RE, &search_string)) {
        state->set_search_string(search_string);
        state->function = &constant_equals_fn;
    } else if (RE2::FullMatch(pattern_str, STARTS_WITH_RE, &search_string)) {
        state->set_search_string(search_string);
        state->function = &constant_starts_with_fn;
    } else if (RE2::FullMatch(pattern_str, ENDS_WITH_RE, &search_string)) {
        state->set_search_string(search_string);
        state->function = &constant_ends_with_fn;
    } else if (RE2::FullMatch(pattern_str, SUBSTRING_RE, &search_string)) {
        state->set_search_string(search_string);
        state->function = &constant_substring_fn;
    } else {
        RETURN_IF_ERROR(compile_with_hyperscan_or_re2<false>(pattern_str, state, context, pattern));
    }

    return Status::OK();
}

Status LikePredicate::regex_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto* state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete state;
    }
    return Status::OK();
}

StatusOr<ColumnPtr> LikePredicate::regex(FunctionContext* context, const Columns& columns) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    return (state->function)(context, columns);
}

// like_fn
StatusOr<ColumnPtr> LikePredicate::like_fn(FunctionContext* context, const starrocks::Columns& columns) {
    return regex_match(context, columns, true);
}

StatusOr<ColumnPtr> LikePredicate::regex_fn(FunctionContext* context, const Columns& columns) {
    return regex_match(context, columns, false);
}

StatusOr<ColumnPtr> LikePredicate::like_fn_with_long_constant_pattern(FunctionContext* context,
                                                                      const Columns& columns) {
    return match_fn_with_long_constant_pattern<true>(context, columns);
}

StatusOr<ColumnPtr> LikePredicate::regex_fn_with_long_constant_pattern(FunctionContext* context,
                                                                       const Columns& columns) {
    return match_fn_with_long_constant_pattern<false>(context, columns);
}

template <bool full_match>
StatusOr<ColumnPtr> LikePredicate::match_fn_with_long_constant_pattern(FunctionContext* context,
                                                                       const Columns& columns) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    const auto& value_column = VECTORIZED_FN_ARGS(0);
    auto [all_const, num_rows] = ColumnHelper::num_packed_rows(columns);

    ColumnViewer<TYPE_VARCHAR> value_viewer(value_column);
    ColumnBuilder<TYPE_BOOLEAN> result(num_rows);

    for (int row = 0; row < num_rows; ++row) {
        if (value_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        bool v = false;
        if constexpr (full_match) {
            v = RE2::FullMatch(re2::StringPiece(value_viewer.value(row).data, value_viewer.value(row).size),
                               *(state->re2));
        } else {
            v = RE2::PartialMatch(re2::StringPiece(value_viewer.value(row).data, value_viewer.value(row).size),
                                  *(state->re2));
        }
        result.append(v);
    }

    return result.build(all_const);
}

// constant_ends
DEFINE_BINARY_FUNCTION_WITH_IMPL(ConstantEndsImpl, value, pattern) {
    return (value.size >= pattern.size) && (pattern == Slice(value.data + value.size - pattern.size, pattern.size));
}

StatusOr<ColumnPtr> LikePredicate::constant_ends_with_fn(FunctionContext* context, const starrocks::Columns& columns) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    const auto& value = VECTORIZED_FN_ARGS(0);
    auto pattern = state->_search_string_column;

    return VectorizedStrictBinaryFunction<ConstantEndsImpl>::evaluate<TYPE_VARCHAR, TYPE_BOOLEAN>(value, pattern);
}

// constant_starts
DEFINE_BINARY_FUNCTION_WITH_IMPL(ConstantStartsImpl, value, pattern) {
    return (value.size >= pattern.size) && (pattern == Slice(value.data, pattern.size));
}

StatusOr<ColumnPtr> LikePredicate::constant_starts_with_fn(FunctionContext* context,
                                                           const starrocks::Columns& columns) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    const auto& value = VECTORIZED_FN_ARGS(0);
    auto pattern = state->_search_string_column;

    return VectorizedStrictBinaryFunction<ConstantStartsImpl>::evaluate<TYPE_VARCHAR, TYPE_BOOLEAN>(value, pattern);
}

// constant_equals
DEFINE_BINARY_FUNCTION_WITH_IMPL(ConstantEqualsImpl, value, pattern) {
    return value == pattern;
}

StatusOr<ColumnPtr> LikePredicate::constant_equals_fn(FunctionContext* context, const starrocks::Columns& columns) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    const auto& value = VECTORIZED_FN_ARGS(0);
    auto pattern = state->_search_string_column;

    return VectorizedStrictBinaryFunction<ConstantEqualsImpl>::evaluate<TYPE_VARCHAR, TYPE_BOOLEAN>(value, pattern);
}

StatusOr<ColumnPtr> LikePredicate::constant_substring_fn(FunctionContext* context, const starrocks::Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    Slice needle = ColumnHelper::get_const_value<TYPE_VARCHAR>(state->_search_string_column);
    auto res = RunTimeColumnType<TYPE_BOOLEAN>::create();

    if (columns[0]->is_constant()) {
        Slice haystack = ColumnHelper::get_const_value<TYPE_VARCHAR>(columns[0]);
        /// It is assumed that the StringSearcher is not very difficult to initialize.
        auto searcher = LibcASCIICaseSensitiveStringSearcher(needle.data, needle.size);

        /// searcher returns a pointer to the found substring or to the end of `haystack`.
        const char* res_pointer = searcher.search(haystack.data, haystack.size);
        if (!res_pointer) {
            res->append(false);
        } else {
            res->append(true);
        }
        return ConstColumn::create(std::move(res), columns[0]->size());
    }

    BinaryColumn* haystack = nullptr;
    NullColumnPtr res_null = nullptr;
    if (columns[0]->is_nullable()) {
        auto haystack_null = ColumnHelper::as_column<NullableColumn>(columns[0]);
        haystack = ColumnHelper::as_raw_column<BinaryColumn>(haystack_null->data_column());

        res_null = haystack_null->null_column();
    } else {
        haystack = ColumnHelper::as_raw_column<BinaryColumn>(columns[0]);
    }

    if (needle.size == 0) {
        // if needle is empty string, every haystack can be matched.
        res->resize(haystack->size());
        size_t type_size = res->type_size();
        memset(res->mutable_raw_data(), 1, res->size() * type_size);
    } else {
        const Buffer<uint32_t>& offsets = haystack->get_offset();
        res->resize(haystack->size());

        const char* begin = haystack->get_slice(0).data;
        const char* pos = begin;
        const char* end = pos + haystack->get_bytes().size();

        /// Current index in the array of strings.
        size_t i = 0;

        auto searcher = VolnitskyUTF8(needle.data, needle.size, end - pos);
        /// We will search for the next occurrence in all strings at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos))) {
            /// Determine which index it refers to.
            while (begin + offsets[i + 1] <= pos) {
                res->get_data()[i] = false;
                ++i;
            }
            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + needle.size > begin + offsets[i + 1]) {
                res->get_data()[i] = false;
            } else {
                res->get_data()[i] = true;
            }
            pos = begin + offsets[i + 1];
            ++i;
        }

        if (i < res->size()) {
            size_t type_size = res->type_size();
            memset(res->mutable_raw_data() + i * type_size, 0, (res->size() - i) * type_size);
        }
    }

    if (columns[0]->has_null()) {
        return NullableColumn::create(std::move(res), std::move(res_null));
    }
    return res;
}

// regex_match
StatusOr<ColumnPtr> LikePredicate::regex_match(FunctionContext* context, const starrocks::Columns& columns,
                                               bool is_like_pattern) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    if (is_like_pattern) {
        return regex_match_full(context, columns);
    } else {
        return regex_match_partial(context, columns);
    }
}

StatusOr<ColumnPtr> LikePredicate::_predicate_const_regex(FunctionContext* context, ColumnBuilder<TYPE_BOOLEAN>* result,
                                                          const ColumnViewer<TYPE_VARCHAR>& value_viewer,
                                                          const ColumnPtr& value_column) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    hs_scratch_t* scratch = nullptr;
    hs_error_t status;
    if ((status = hs_clone_scratch(state->scratch, &scratch)) != HS_SUCCESS) {
        return Status::InternalError(fmt::format("unable to clone scratch space, status: {}", status));
    }

    DeferOp op([&] {
        if (scratch != nullptr) {
            hs_error_t st;
            if ((st = hs_free_scratch(scratch)) != HS_SUCCESS) {
                LOG(ERROR) << "free scratch space failure. status: " << st;
            }
        }
    });

    for (int row = 0; row < value_viewer.size(); ++row) {
        if (value_viewer.is_null(row)) {
            result->append_null();
            continue;
        }

        bool v = false;
        auto value_size = value_viewer.value(row).size;
        [[maybe_unused]] auto status = hs_scan(
                // Use &_DUMMY_STRING_FOR_EMPTY_PATTERN instead of nullptr to avoid crash.
                state->database, (value_size) ? value_viewer.value(row).data : &_DUMMY_STRING_FOR_EMPTY_PATTERN,
                value_size, 0, scratch,
                [](unsigned int id, unsigned long long from, unsigned long long to, unsigned int flags,
                   void* ctx) -> int {
                    *((bool*)ctx) = true;
                    return 1;
                },
                &v);

        DCHECK(status == HS_SUCCESS || status == HS_SCAN_TERMINATED) << " status: " << status;
        result->append(v);
    }

    return result->build(value_column->is_constant());
}

enum class FastPathType {
    EQUALS = 0,
    START_WITH = 1,
    END_WITH = 2,
    SUBSTRING = 3,
    REGEX = 4,
};

FastPathType extract_fast_path(const Slice& pattern) {
    if (pattern.empty() || pattern.size < 2) {
        return FastPathType::REGEX;
    }

    if (pattern.data[0] == '_' || pattern.data[pattern.size - 1] == '_') {
        return FastPathType::REGEX;
    }

    bool is_end_with = pattern.data[0] == '%';
    bool is_start_with = pattern.data[pattern.size - 1] == '%';

    for (size_t i = 1; i < pattern.size - 1;) {
        if (pattern.data[i] == '\\') {
            i += 2;
        } else {
            if (pattern.data[i] == '%' || pattern.data[i] == '_') {
                return FastPathType::REGEX;
            }
            i++;
        }
    }

    if (is_end_with && is_start_with) {
        return FastPathType::SUBSTRING;
    } else if (is_end_with) {
        return FastPathType::END_WITH;
    } else if (is_start_with) {
        return FastPathType::START_WITH;
    } else {
        return FastPathType::EQUALS;
    }
}

StatusOr<ColumnPtr> LikePredicate::regex_match_full(FunctionContext* context, const starrocks::Columns& columns) {
    const auto& value_column = VECTORIZED_FN_ARGS(0);
    const auto& pattern_column = VECTORIZED_FN_ARGS(1);
    auto [all_const, num_rows] = ColumnHelper::num_packed_rows(columns);

    ColumnViewer<TYPE_VARCHAR> value_viewer(value_column);
    ColumnBuilder<TYPE_BOOLEAN> result(num_rows);

    // pattern is constant value, use context's regex
    if (context->is_constant_column(1)) {
        if (!pattern_column->only_null()) {
            return _predicate_const_regex(context, &result, value_viewer, value_column);
        } else {
            // because pattern_column is constant, so if it is nullable means it is only_null.
            return ColumnHelper::create_const_null_column(value_column->size());
        }
    }

    ColumnViewer<TYPE_VARCHAR> pattern_viewer(pattern_column);

    RE2::Options opts;
    opts.set_never_nl(false);
    opts.set_dot_nl(true);
    opts.set_log_errors(false);

    for (int row = 0; row < num_rows; ++row) {
        if (value_viewer.is_null(row) || pattern_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        Slice pattern = pattern_viewer.value(row);
        FastPathType val = extract_fast_path(pattern);
        switch (val) {
        case FastPathType::EQUALS: {
            std::string str_pattern = pattern.to_string();
            remove_escape_character(&str_pattern);
            result.append(value_viewer.value(row) == str_pattern);
            break;
        }
        case FastPathType::START_WITH: {
            std::string str_pattern = pattern.to_string();
            remove_escape_character(&str_pattern);
            auto pattern_slice = Slice(str_pattern);
            pattern_slice.remove_suffix(1);
            result.append(ConstantStartsImpl::apply<Slice, Slice, bool>(value_viewer.value(row), pattern_slice));
            break;
        }
        case FastPathType::END_WITH: {
            std::string str_pattern = pattern.to_string();
            remove_escape_character(&str_pattern);
            auto pattern_slice = Slice(str_pattern);
            pattern_slice.remove_prefix(1);
            result.append(ConstantEndsImpl::apply<Slice, Slice, bool>(value_viewer.value(row), pattern_slice));
            break;
        }
        case FastPathType::SUBSTRING: {
            std::string str_pattern = pattern.to_string();
            remove_escape_character(&str_pattern);
            auto pattern_slice = Slice(str_pattern);
            pattern_slice.remove_prefix(1);
            pattern_slice.remove_suffix(1);
            auto searcher = LibcASCIICaseSensitiveStringSearcher(pattern_slice.get_data(), pattern_slice.get_size());
            /// searcher returns a pointer to the found substring or to the end of `haystack`.
            const Slice& value = value_viewer.value(row);
            const char* res_pointer = searcher.search(value.data, value.size);
            result.append(!!res_pointer);
            break;
        }
        case FastPathType::REGEX: {
            auto re_pattern = LikePredicate::template convert_like_pattern<false>(context, pattern);

            re2::RE2 re(re_pattern, opts);
            if (!re.ok()) {
                return Status::InvalidArgument(strings::Substitute("Invalid regex: $0", re_pattern));
            }
            auto v = RE2::FullMatch(re2::StringPiece(value_viewer.value(row).data, value_viewer.value(row).size), re);
            result.append(v);
            break;
        }
        }
    }

    return result.build(all_const);
}

StatusOr<ColumnPtr> LikePredicate::regex_match_partial(FunctionContext* context, const starrocks::Columns& columns) {
    const auto& value_column = VECTORIZED_FN_ARGS(0);
    const auto& pattern_column = VECTORIZED_FN_ARGS(1);
    auto [all_const, num_rows] = ColumnHelper::num_packed_rows(columns);

    ColumnViewer<TYPE_VARCHAR> value_viewer(value_column);
    ColumnBuilder<TYPE_BOOLEAN> result(num_rows);

    // pattern is constant value, use context's regex
    if (context->is_constant_column(1)) {
        if (!pattern_column->only_null()) {
            return _predicate_const_regex(context, &result, value_viewer, value_column);
        } else {
            // because pattern_column is constant, so if it is nullable means it is only_null.
            return ColumnHelper::create_const_null_column(value_column->size());
        }
    }

    ColumnViewer<TYPE_VARCHAR> pattern_viewer(pattern_column);

    RE2::Options opts;
    opts.set_never_nl(false);
    opts.set_dot_nl(true);
    opts.set_log_errors(false);

    for (int row = 0; row < num_rows; ++row) {
        if (value_viewer.is_null(row) || pattern_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto re_pattern = pattern_viewer.value(row).to_string();

        re2::RE2 re(re_pattern, opts);

        if (!re.ok()) {
            context->set_error(strings::Substitute("Invalid regex: $0", re_pattern).c_str());
            result.append_null();
            continue;
        }

        auto v = RE2::PartialMatch(re2::StringPiece(value_viewer.value(row).data, value_viewer.value(row).size), re);
        result.append(v);
    }

    return result.build(all_const);
}

template <bool fullMatch>
std::string LikePredicate::convert_like_pattern(FunctionContext* context, const Slice& pattern) {
    std::string re_pattern;
    re_pattern.clear();

    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    bool is_escaped = false;

    if constexpr (fullMatch) {
        re_pattern.append("^");
    }

    for (int i = 0; i < pattern.size; ++i) {
        if (!is_escaped && pattern.data[i] == '%') {
            re_pattern.append(".*");
        } else if (!is_escaped && pattern.data[i] == '_') {
            re_pattern.append(".");
            // check for escape char before checking for regex special chars, they might overlap
        } else if (!is_escaped && pattern.data[i] == state->escape_char) {
            is_escaped = true;
        } else if (pattern.data[i] == '.' || pattern.data[i] == '[' || pattern.data[i] == ']' ||
                   pattern.data[i] == '{' || pattern.data[i] == '}' || pattern.data[i] == '(' ||
                   pattern.data[i] == ')' || pattern.data[i] == '\\' || pattern.data[i] == '*' ||
                   pattern.data[i] == '+' || pattern.data[i] == '?' || pattern.data[i] == '|' ||
                   pattern.data[i] == '^' || pattern.data[i] == '$') {
            // escape all regex special characters; see list at
            re_pattern.append("\\");
            re_pattern.append(1, pattern.data[i]);
            is_escaped = false;
        } else {
            // regular character or escaped special character
            re_pattern.append(1, pattern.data[i]);
            is_escaped = false;
        }
    }

    if constexpr (fullMatch) {
        re_pattern.append("$");
    }

    return re_pattern;
}

void LikePredicate::remove_escape_character(std::string* search_string) {
    std::string tmp_search_string;
    tmp_search_string.swap(*search_string);
    int len = tmp_search_string.length();
    for (int i = 0; i < len;) {
        if (tmp_search_string[i] == '\\' && i + 1 < len &&
            (tmp_search_string[i + 1] == '%' || tmp_search_string[i + 1] == '_')) {
            search_string->append(1, tmp_search_string[i + 1]);
            i += 2;
        } else {
            search_string->append(1, tmp_search_string[i]);
            i++;
        }
    }
}

} // namespace starrocks
