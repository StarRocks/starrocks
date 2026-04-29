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

#include "base/format.h"

auto fmt::formatter<starrocks::CompressionTypePB>::format(const starrocks::CompressionTypePB value,
                                                          format_context& ctx) const -> format_context::iterator {
    return formatter<std::underlying_type_t<starrocks::CompressionTypePB>>::format(
            starrocks::enum_to_underlying_type(value), ctx);
}

auto fmt::formatter<starrocks::EncryptionAlgorithmPB>::format(const starrocks::EncryptionAlgorithmPB value,
                                                              format_context& ctx) const -> format_context::iterator {
    return formatter<std::underlying_type_t<starrocks::EncryptionAlgorithmPB>>::format(
            starrocks::enum_to_underlying_type(value), ctx);
}

auto fmt::formatter<starrocks::EncryptionKeyTypePB>::format(const starrocks::EncryptionKeyTypePB value,
                                                            format_context& ctx) const -> format_context::iterator {
    return formatter<std::underlying_type_t<starrocks::EncryptionKeyTypePB>>::format(
            starrocks::enum_to_underlying_type(value), ctx);
}

auto fmt::formatter<starrocks::NullEncodingPB>::format(const starrocks::NullEncodingPB value, format_context& ctx) const
        -> format_context::iterator {
    return formatter<std::underlying_type_t<starrocks::NullEncodingPB>>::format(
            starrocks::enum_to_underlying_type(value), ctx);
}

auto fmt::formatter<starrocks::TStatusCode::type>::format(const starrocks::TStatusCode::type value,
                                                          format_context& ctx) const -> format_context::iterator {
    return formatter<std::underlying_type_t<starrocks::TStatusCode::type>>::format(
            starrocks::enum_to_underlying_type(value), ctx);
}

auto fmt::formatter<starrocks::TExprNodeType::type>::format(const starrocks::TExprNodeType::type value,
                                                            format_context& ctx) const -> format_context::iterator {
    return formatter<std::underlying_type_t<starrocks::TExprNodeType::type>>::format(
            starrocks::enum_to_underlying_type(value), ctx);
}

auto fmt::formatter<starrocks::TFunctionBinaryType::type>::format(const starrocks::TFunctionBinaryType::type value,
                                                                  format_context& ctx) const
        -> format_context::iterator {
    return formatter<std::underlying_type_t<starrocks::TFunctionBinaryType::type>>::format(
            starrocks::enum_to_underlying_type(value), ctx);
}

auto fmt::formatter<starrocks::SampleMethod::type>::format(const starrocks::SampleMethod::type value,
                                                           format_context& ctx) const -> format_context::iterator {
    return formatter<std::underlying_type_t<starrocks::SampleMethod::type>>::format(
            starrocks::enum_to_underlying_type(value), ctx);
}

auto fmt::formatter<starrocks::TFileType::type>::format(const starrocks::TFileType::type value,
                                                        format_context& ctx) const -> format_context::iterator {
    return formatter<std::underlying_type_t<starrocks::TFileType::type>>::format(
            starrocks::enum_to_underlying_type(value), ctx);
}

auto fmt::formatter<orc::TypeKind>::format(const orc::TypeKind value, format_context& ctx) const
        -> format_context::iterator {
    return formatter<std::underlying_type_t<orc::TypeKind>>::format(starrocks::enum_to_underlying_type(value), ctx);
}

auto fmt::formatter<tparquet::Type::type>::format(const tparquet::Type::type value, format_context& ctx) const
        -> format_context::iterator {
    return formatter<std::string>::format(tparquet::to_string(value), ctx);
}
