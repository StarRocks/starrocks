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

// Minimal fmt helpers for macOS build
// Purpose: Enable fmt's ostream support so fmt::format can print StarRocks
// types/enums that define operator<< in existing headers, without modifying core code.

#pragma once

// Skip fmt shims when specifically requested to avoid template conflicts
#ifdef STARROCKS_DISABLE_FMT_SHIMS
#define STARROCKS_SKIP_FMT_SHIMS
#endif

// Also skip shims for any file that includes RocksDB headers to avoid template conflicts
#ifdef ROCKSDB_LITE
#define STARROCKS_SKIP_FMT_SHIMS
#endif

// Skip for files that might include RocksDB indirectly through other headers
#if defined(ROCKSDB_DB_H) || defined(ROCKSDB_OPTIONS_H) || defined(ROCKSDB_SLICE_H)
#define STARROCKS_SKIP_FMT_SHIMS
#endif

// Skip for any files that might contain storage-related RocksDB dependencies
// This is a broad approach to catch remaining problematic files at the end of build
#if defined(__INCLUDE_STORAGES__) || defined(STORAGE_KV_STORE_H) || defined(ROCKSDB_STATUS_ADAPTER_H)
#define STARROCKS_SKIP_FMT_SHIMS
#endif

#ifndef STARROCKS_SKIP_FMT_SHIMS

#include <fmt/format.h>
// Note: fmt/ostream.h is excluded on macOS to avoid RocksDB incomplete type issues
// Enable fmt::join and range printing
#include <fmt/ranges.h>
#include <pthread.h>
#include <thread>
#include <sstream>
#include <string>
#include <algorithm>

// Bring in StarRocks enum definitions and helpers
#include "gen_cpp/Types_types.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/StatusCode_types.h"
#include "gen_cpp/MVMaintenance_types.h"
#include "types/logical_type.h"
#include <atomic>
#include <type_traits>
#include <cstdlib> // ensure integer std::abs overloads are visible on libc++
#include <string_view>
#include <libgen.h> // ensure basename() is declared on macOS

// Fix for typeof compatibility on Clang/macOS - typeof is GCC extension
#ifndef typeof
#define typeof __typeof__
#endif

// Prefer the lightweight format_as customization point added in fmt >= 10.
// It lets fmt format our types via ADL by converting them to a formattable type.
namespace starrocks {

inline const char* format_as(LogicalType v) {
  return logical_type_to_string(v);
}

template <typename E>
inline auto format_as(E v) -> decltype(to_string(v)) {
  return to_string(v);
}

} // namespace starrocks

// Provide a generic format_as for any enum declared under starrocks (and subnamespaces)
// so that fmt can format enums via their underlying integer type without
// needing per-enum formatters. This avoids touching core code.
namespace starrocks {
template <typename E, std::enable_if_t<std::is_enum_v<E>, int> = 0>
inline auto format_as(E e) -> std::underlying_type_t<E> {
  using U = std::underlying_type_t<E>;
  return static_cast<U>(e);
}
} // namespace starrocks

namespace starrocks::pipeline {
template <typename E, std::enable_if_t<std::is_enum_v<E>, int> = 0>
inline auto format_as(E e) -> std::underlying_type_t<E> {
  using U = std::underlying_type_t<E>;
  return static_cast<U>(e);
}
} // namespace starrocks::pipeline

// libc++ on macOS has stricter template deduction for std::max than libstdc++.
// Some StarRocks call sites pass a mix of long and long long which fails to deduce.
// Provide interop overloads to resolve those calls without touching core sources.
#include <algorithm>
namespace std {
inline constexpr long long max(long a, long long b) {
  return std::max<long long>(static_cast<long long>(a), b);
}
inline constexpr long long max(long long a, long b) {
  return std::max<long long>(a, static_cast<long long>(b));
}
// Provide std::abs overload for extended integer __int128 (missing in libc++)
inline __int128 abs(__int128 v) { return v < 0 ? -v : v; }
} // namespace std

// Provide fmt formatter for std::atomic<T> by formatting the loaded value.
template <typename T>
struct fmt::formatter<std::atomic<T>> : fmt::formatter<T> {
  template <typename FormatContext>
  auto format(const std::atomic<T>& v, FormatContext& ctx) const {
    return fmt::formatter<T>::format(v.load(), ctx);
  }
};

// Generic fallback: format any enum as its underlying integer when
// there is no dedicated formatter. This avoids errors like
// "type_is_unformattable_for<Enum, char>" on macOS' libc++.
// Note: We intentionally avoid specializing fmt::formatter for all enums here
// because fmt::formatter has only two template parameters in most fmt versions.
// The format_as() overloads above are sufficient and safer.

// However, some external enums (not found by ADL or older fmt versions)
// still fail. Provide narrow specializations for the ones used with fmt::format.
namespace fmt {
template <>
struct formatter<starrocks::TFileType::type> : formatter<int> {
  template <typename FormatContext>
  auto format(starrocks::TFileType::type v, FormatContext& ctx) const {
    return formatter<int>::format(static_cast<int>(v), ctx);
  }
};

template <>
struct formatter<starrocks::StreamSourceType::type> : formatter<int> {
  template <typename FormatContext>
  auto format(starrocks::StreamSourceType::type v, FormatContext& ctx) const {
    return formatter<int>::format(static_cast<int>(v), ctx);
  }
};

// Formatter for thrift enum used in ArrowFunctionCall
template <>
struct formatter<starrocks::TFunctionBinaryType::type> : formatter<int> {
  template <typename FormatContext>
  auto format(starrocks::TFunctionBinaryType::type v, FormatContext& ctx) const {
    return formatter<int>::format(static_cast<int>(v), ctx);
  }
};

// Formatter for TExprNodeType thrift enum used in expr.cpp
template <>
struct formatter<starrocks::TExprNodeType::type> : formatter<int> {
  template <typename FormatContext>
  auto format(starrocks::TExprNodeType::type v, FormatContext& ctx) const {
    return formatter<int>::format(static_cast<int>(v), ctx);
  }
};

// Formatter for TStatusCode thrift enum used in compaction_action.cpp
template <>
struct formatter<starrocks::TStatusCode::type> : formatter<int> {
  template <typename FormatContext>
  auto format(starrocks::TStatusCode::type v, FormatContext& ctx) const {
    return formatter<int>::format(static_cast<int>(v), ctx);
  }
};

// Formatter for MVTaskType thrift enum used in internal_service.cpp
template <>
struct formatter<starrocks::MVTaskType::type> : formatter<int> {
  template <typename FormatContext>
  auto format(starrocks::MVTaskType::type v, FormatContext& ctx) const {
    return formatter<int>::format(static_cast<int>(v), ctx);
  }
};

// Formatter for SampleMethod thrift enum used in segment_iterator.cpp
template <>
struct formatter<starrocks::SampleMethod::type> : formatter<int> {
  template <typename FormatContext>
  auto format(starrocks::SampleMethod::type v, FormatContext& ctx) const {
    return formatter<int>::format(static_cast<int>(v), ctx);
  }
};

} // namespace fmt




// Provide a minimal fallback for boost::algorithm::to_lower_copy used in one
// code path without pulling the entire Boost algorithm headers which cause
// template conflicts on macOS with certain containers.
namespace boost { namespace algorithm {
inline std::string to_lower_copy(const std::string& s) {
  std::string r = s;
  std::transform(r.begin(), r.end(), r.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return r;
}
}} // namespace boost::algorithm

// Fix for boost::algorithm::join issues on macOS
// The issue is that boost::algorithm::join cannot handle Buffer<uint8_t> properly
// We override the problematic functions to work with basic types

namespace boost { namespace algorithm {
// Override boost::algorithm::join only for non-string sequences to avoid template issues
template <typename Sequence, typename Separator>
auto join(const Sequence& sequence, const Separator& separator)
    -> std::enable_if_t<!std::is_same_v<typename Sequence::value_type, std::string>, std::string> {
    std::string result;
    auto it = sequence.begin();
    auto end = sequence.end();

    if (it != end) {
        result += std::to_string(*it);
        ++it;
    }

    for (; it != end; ++it) {
        result += separator;
        result += std::to_string(*it);
    }

    return result;
}
}} // namespace boost::algorithm

// Fix for std::any_of with isspace on macOS
// isspace is overloaded in macOS, causing template deduction issues
namespace starrocks_macos_shims {
    inline bool isspace_wrapper(int c) {
        return std::isspace(c);
    }
}

// Redefine isspace for use with std::any_of only on macOS
#ifdef __APPLE__
#define STARROCKS_ISSPACE(c) starrocks_macos_shims::isspace_wrapper(c)
#else
#define STARROCKS_ISSPACE(c) std::isspace(c)
#endif

// Additional formatters for problematic types on macOS

// Formatter for pthread_t
template <>
struct fmt::formatter<pthread_t> : formatter<uintptr_t> {
  template <typename FormatContext>
  auto format(pthread_t v, FormatContext& ctx) const {
    return formatter<uintptr_t>::format(reinterpret_cast<uintptr_t>(v), ctx);
  }
};

// Formatter for std::thread::id
template <>
struct fmt::formatter<std::thread::id> : formatter<std::string> {
  template <typename FormatContext>
  auto format(const std::thread::id& v, FormatContext& ctx) const {
    std::ostringstream oss;
    oss << v;
    return formatter<std::string>::format(oss.str(), ctx);
  }
};

// Formatter for __int128 (missing in some fmt versions)
template <>
struct fmt::formatter<__int128> {
  constexpr auto parse(fmt::format_parse_context& ctx) -> decltype(ctx.begin()) {
    return ctx.end();
  }

  template <typename FormatContext>
  auto format(__int128 v, FormatContext& ctx) const {
    if (v == 0) {
      return fmt::format_to(ctx.out(), "0");
    }
    bool negative = v < 0;
    if (negative) v = -v;
    std::string result;
    while (v > 0) {
      result.push_back('0' + v % 10);
      v /= 10;
    }
    if (negative) result.push_back('-');
    std::reverse(result.begin(), result.end());
    return fmt::format_to(ctx.out(), "{}", result);
  }
};

// Formatter for unsigned __int128
template <>
struct fmt::formatter<unsigned __int128> {
  constexpr auto parse(fmt::format_parse_context& ctx) -> decltype(ctx.begin()) {
    return ctx.end();
  }

  template <typename FormatContext>
  auto format(unsigned __int128 v, FormatContext& ctx) const {
    if (v == 0) {
      return fmt::format_to(ctx.out(), "0");
    }
    std::string result;
    while (v > 0) {
      result.push_back('0' + v % 10);
      v /= 10;
    }
    std::reverse(result.begin(), result.end());
    return fmt::format_to(ctx.out(), "{}", result);
  }
};

#endif // STARROCKS_SKIP_FMT_SHIMS
