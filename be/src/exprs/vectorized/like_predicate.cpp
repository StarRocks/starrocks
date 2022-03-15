// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/like_predicate.h"

#include <memory>

#include "exprs/vectorized/binary_function.h"
#include "glog/logging.h"
#include "gutil/strings/substitute.h"
#include "runtime/vectorized/Volnitsky.h"

namespace starrocks::vectorized {

// A regex to match any regex pattern is equivalent to a substring search.
static const RE2 SUBSTRING_RE(R"((?:\.\*)*([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)(?:\.\*)*)");

// A regex to match any regex pattern which is equivalent to matching a constant string
// at the end of the string values.
static const RE2 ENDS_WITH_RE(R"((?:\.\*)*([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)\$)");

// A regex to match any regex pattern which is equivalent to matching a constant string
// at the end of the string values.
static const RE2 STARTS_WITH_RE(R"(\^([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)(?:\.\*)*)");

// A regex to match any regex pattern which is equivalent to a constant string match.
static const RE2 EQUALS_RE(R"(\^([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)\$)");

static const re2::RE2 LIKE_SUBSTRING_RE(R"((?:%+)(((\\%)|(\\_)|([^%_]))+)(?:%+))");
static const re2::RE2 LIKE_ENDS_WITH_RE(R"((?:%+)(((\\%)|(\\_)|([^%_]))+))");
static const re2::RE2 LIKE_STARTS_WITH_RE(R"((((\\%)|(\\_)|([^%_]))+)(?:%+))");
static const re2::RE2 LIKE_EQUALS_RE(R"((((\\%)|(\\_)|([^%_]))+))");

Status LikePredicate::hs_compile_and_alloc_scratch(const std::string& pattern, LikePredicateState* state,
                                                   starrocks_udf::FunctionContext* context, const Slice& slice) {
    if (hs_compile(pattern.c_str(), HS_FLAG_ALLOWEMPTY | HS_FLAG_DOTALL | HS_FLAG_UTF8 | HS_FLAG_SINGLEMATCH,
                   HS_MODE_BLOCK, nullptr, &state->database, &state->compile_err) != HS_SUCCESS) {
        std::stringstream error;
        error << "Invalid regex expression: " << slice.data << ": " << state->compile_err->message;
        context->set_error(error.str().c_str());
        hs_free_compile_error(state->compile_err);
        return Status::InvalidArgument(error.str());
    }

    if (hs_alloc_scratch(state->database, &state->scratch) != HS_SUCCESS) {
        std::stringstream error;
        error << "ERROR: Unable to allocate scratch space.";
        context->set_error(error.str().c_str());
        hs_free_database(state->database);
        return Status::InvalidArgument(error.str());
    }

    return Status::OK();
}

// when pattern is one of (EQUALS | SUBSTRING | STARTS_WITH | ENDS_WITH) or variable value
// we use Re2.
// when pattern is a constant value except ((EQUALS | SUBSTRING | STARTS_WITH | ENDS_WITH) variable value)
// we use hyperscan.

// like predicate
Status LikePredicate::like_prepare(starrocks_udf::FunctionContext* context,
                                   starrocks_udf::FunctionContext::FunctionStateScope scope) {
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
        RETURN_IF_ERROR(hs_compile_and_alloc_scratch(re_pattern, state, context, pattern));
    }
    return Status::OK();
}

Status LikePredicate::like_close(starrocks_udf::FunctionContext* context,
                                 starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete state;
    }
    return Status::OK();
}

ColumnPtr LikePredicate::like(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    return (state->function)(context, columns);
}

// regex predicate
Status LikePredicate::regex_prepare(starrocks_udf::FunctionContext* context,
                                    starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }

    // @todo: should replace to mem pool
    LikePredicateState* state = new LikePredicateState();
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
        std::string re_pattern(pattern.data, pattern.size);
        RETURN_IF_ERROR(hs_compile_and_alloc_scratch(re_pattern, state, context, pattern));
    }

    return Status::OK();
}

Status LikePredicate::regex_close(starrocks_udf::FunctionContext* context,
                                  starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        LikePredicateState* state =
                reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete state;
    }
    return Status::OK();
}

ColumnPtr LikePredicate::regex(FunctionContext* context, const Columns& columns) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    return (state->function)(context, columns);
}

// like_fn
ColumnPtr LikePredicate::like_fn(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    return regex_match(context, columns, true);
}

ColumnPtr LikePredicate::regex_fn(FunctionContext* context, const Columns& columns) {
    return regex_match(context, columns, false);
}

// constant_ends
DEFINE_BINARY_FUNCTION_WITH_IMPL(ConstantEndsImpl, value, pattern) {
    return (value.size >= pattern.size) && (pattern == Slice(value.data + value.size - pattern.size, pattern.size));
}

ColumnPtr LikePredicate::constant_ends_with_fn(FunctionContext* context,
                                               const starrocks::vectorized::Columns& columns) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    auto value = VECTORIZED_FN_ARGS(0);
    auto pattern = state->_search_string_column;

    return VectorizedStrictBinaryFunction<ConstantEndsImpl>::evaluate<TYPE_VARCHAR, TYPE_BOOLEAN>(value, pattern);
}

// constant_starts
DEFINE_BINARY_FUNCTION_WITH_IMPL(ConstantStartsImpl, value, pattern) {
    return (value.size >= pattern.size) && (pattern == Slice(value.data, pattern.size));
}

ColumnPtr LikePredicate::constant_starts_with_fn(FunctionContext* context,
                                                 const starrocks::vectorized::Columns& columns) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    auto value = VECTORIZED_FN_ARGS(0);
    auto pattern = state->_search_string_column;

    return VectorizedStrictBinaryFunction<ConstantStartsImpl>::evaluate<TYPE_VARCHAR, TYPE_BOOLEAN>(value, pattern);
}

// constant_equals
DEFINE_BINARY_FUNCTION_WITH_IMPL(ConstantEqualsImpl, value, pattern) {
    return value == pattern;
}

ColumnPtr LikePredicate::constant_equals_fn(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    auto value = VECTORIZED_FN_ARGS(0);
    auto pattern = state->_search_string_column;

    return VectorizedStrictBinaryFunction<ConstantEqualsImpl>::evaluate<TYPE_VARCHAR, TYPE_BOOLEAN>(value, pattern);
}

ColumnPtr LikePredicate::constant_substring_fn(FunctionContext* context,
                                               const starrocks::vectorized::Columns& columns) {
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
        return ConstColumn::create(res, columns[0]->size());
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
        const std::vector<uint32_t>& offsets = haystack->get_offset();
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
        return NullableColumn::create(res, res_null);
    }
    return res;
}

// regex_match
ColumnPtr LikePredicate::regex_match(FunctionContext* context, const starrocks::vectorized::Columns& columns,
                                     bool is_like_pattern) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    if (is_like_pattern) {
        return regex_match_full(context, columns);
    } else {
        return regex_match_partial(context, columns);
    }
}

ColumnPtr LikePredicate::_predicate_const_regex(FunctionContext* context, ColumnBuilder<TYPE_BOOLEAN>* result,
                                                const ColumnViewer<TYPE_VARCHAR>& value_viewer,
                                                const ColumnPtr& value_column) {
    auto state = reinterpret_cast<LikePredicateState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    hs_scratch_t* scratch = nullptr;
    hs_error_t status;
    if ((status = hs_clone_scratch(state->scratch, &scratch)) != HS_SUCCESS) {
        CHECK(false) << "ERROR: Unable to clone scratch space."
                     << " status: " << status;
    }

    for (int row = 0; row < value_viewer.size(); ++row) {
        if (value_viewer.is_null(row)) {
            result->append_null();
            continue;
        }

        bool v = false;
        auto value_size = value_viewer.value(row).size;
        auto status = hs_scan(
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

    if ((status = hs_free_scratch(scratch)) != HS_SUCCESS) {
        CHECK(false) << "ERROR: free scratch space failure"
                     << " status: " << status;
    }
    return result->build(value_column->is_constant());
}

ColumnPtr LikePredicate::regex_match_full(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    auto value_column = VECTORIZED_FN_ARGS(0);
    auto pattern_column = VECTORIZED_FN_ARGS(1);

    ColumnViewer<TYPE_VARCHAR> value_viewer(value_column);
    ColumnBuilder<TYPE_BOOLEAN> result(value_viewer.size());

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

    for (int row = 0; row < value_viewer.size(); ++row) {
        if (value_viewer.is_null(row) || pattern_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto re_pattern = LikePredicate::template convert_like_pattern<false>(context, pattern_viewer.value(row));

        re2::RE2 re(re_pattern, opts);

        if (!re.ok()) {
            context->set_error(strings::Substitute("Invalid regex: $0", re_pattern).c_str());
            result.append_null();
            continue;
        }

        auto v = RE2::FullMatch(re2::StringPiece(value_viewer.value(row).data, value_viewer.value(row).size), re);
        result.append(v);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr LikePredicate::regex_match_partial(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    auto value_column = VECTORIZED_FN_ARGS(0);
    auto pattern_column = VECTORIZED_FN_ARGS(1);

    ColumnViewer<TYPE_VARCHAR> value_viewer(value_column);
    ColumnBuilder<TYPE_BOOLEAN> result(value_viewer.size());

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

    for (int row = 0; row < value_viewer.size(); ++row) {
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

    return result.build(ColumnHelper::is_all_const(columns));
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

} // namespace starrocks::vectorized
