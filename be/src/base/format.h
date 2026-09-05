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

#include <orc/OrcFile.hh> // for TypeKind
#include <string>
#include <string_view>
#include <type_traits>

#include "gen_cpp/Exprs_types.h"      // for TExprNodeType
#include "gen_cpp/PlanNodes_types.h"  // for SampleMethod
#include "gen_cpp/StatusCode_types.h" // for TStatusCode
#include "gen_cpp/Types_types.h"      // for TFunctionBinaryType, TFileType
#include "gen_cpp/encryption.pb.h"    // for EncryptionAlgorithmPB, EncryptionKeyTypePB
#include "gen_cpp/parquet_types.h"
#include "gen_cpp/segment.pb.h" // for NullEncodingPB
#include "gen_cpp/types.pb.h"   // for CompressionTypePB

/// Custom formatters for various commonly used types of external libraries (proto, thrift, orc, parquet, ...)
namespace starrocks {

template <typename E>
requires std::is_enum_v<E>[[nodiscard]] constexpr auto enum_to_underlying_type(E enumerator) noexcept {
    return static_cast<std::underlying_type_t<E>>(enumerator);
}

} // namespace starrocks

template <>
struct fmt::formatter<starrocks::CompressionTypePB> : formatter<std::underlying_type_t<starrocks::CompressionTypePB>> {
    auto format(starrocks::CompressionTypePB value, format_context& ctx) const -> format_context::iterator;
};

template <>
struct fmt::formatter<starrocks::EncryptionAlgorithmPB>
        : formatter<std::underlying_type_t<starrocks::EncryptionAlgorithmPB>> {
    auto format(starrocks::EncryptionAlgorithmPB value, format_context& ctx) const -> format_context::iterator;
};

template <>
struct fmt::formatter<starrocks::EncryptionKeyTypePB>
        : formatter<std::underlying_type_t<starrocks::EncryptionKeyTypePB>> {
    auto format(starrocks::EncryptionKeyTypePB value, format_context& ctx) const -> format_context::iterator;
};

template <>
struct fmt::formatter<starrocks::NullEncodingPB> : formatter<std::underlying_type_t<starrocks::NullEncodingPB>> {
    auto format(starrocks::NullEncodingPB value, format_context& ctx) const -> format_context::iterator;
};

template <>
struct fmt::formatter<starrocks::TStatusCode::type> : formatter<std::underlying_type_t<starrocks::TStatusCode::type>> {
    auto format(starrocks::TStatusCode::type value, format_context& ctx) const -> format_context::iterator;
};

template <>
struct fmt::formatter<starrocks::TExprNodeType::type>
        : formatter<std::underlying_type_t<starrocks::TExprNodeType::type>> {
    auto format(starrocks::TExprNodeType::type value, format_context& ctx) const -> format_context::iterator;
};

template <>
struct fmt::formatter<starrocks::TFunctionBinaryType::type>
        : formatter<std::underlying_type_t<starrocks::TFunctionBinaryType::type>> {
    auto format(starrocks::TFunctionBinaryType::type value, format_context& ctx) const -> format_context::iterator;
};

template <>
struct fmt::formatter<starrocks::SampleMethod::type>
        : formatter<std::underlying_type_t<starrocks::SampleMethod::type>> {
    auto format(starrocks::SampleMethod::type value, format_context& ctx) const -> format_context::iterator;
};

template <>
struct fmt::formatter<starrocks::TFileType::type> : formatter<std::underlying_type_t<starrocks::TFileType::type>> {
    auto format(starrocks::TFileType::type value, format_context& ctx) const -> format_context::iterator;
};

template <>
struct fmt::formatter<orc::TypeKind> : formatter<std::underlying_type_t<orc::TypeKind>> {
    auto format(orc::TypeKind value, format_context& ctx) const -> format_context::iterator;
};

template <>
struct fmt::formatter<tparquet::Type::type> : formatter<std::string> {
    auto format(tparquet::Type::type value, format_context& ctx) const -> format_context::iterator;
};
