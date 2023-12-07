#pragma once

#include <memory>

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
template <size_t... Is>
struct index_list {};

// Declare primary template for index range builder
template <size_t MIN, size_t N, size_t... Is>
struct range_builder;

// Base step
template <size_t MIN, size_t... Is>
struct range_builder<MIN, MIN, Is...> {
    typedef index_list<Is...> type;
};

// Induction step
template <size_t MIN, size_t N, size_t... Is>
struct range_builder : public range_builder<MIN, N - 1, N - 1, Is...> {};

// Meta-function that returns a [MIN, MAX) index range
template <size_t MIN, size_t MAX>
using index_range = typename detail::range_builder<MIN, MAX>::type;
} // namespace detail
#endif
} // namespace wrenbind17
