// src/types/int256.h
#pragma once

#include <cstdint>
#include <type_traits>
#include <string>
#include <fmt/format.h>
#include <boost/multiprecision/cpp_int.hpp>

namespace starrocks {

using int256_t = boost::multiprecision::int256_t;

inline std::string to_string(const int256_t& value) {
    return value.str();
}

} // namespace starrocks