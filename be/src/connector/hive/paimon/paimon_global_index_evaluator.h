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

#include <paimon/global_index/global_index_reader.h>
#include <paimon/global_index/global_index_result.h>
#include <rapidjson/document.h>

#include <functional>
#include <memory>
#include <string_view>
#include <utility>

#include "common/statusor.h"

namespace starrocks {

// Evaluates the compact ScalarOperator JSON emitted by ScalarOperatorSerializer.
// Reader selection is injected so the scanner can bind every column to the exact
// index implementation whose snapshot coverage was validated by FE.
class PaimonGlobalIndexEvaluator {
public:
    using ReaderGetter =
            std::function<StatusOr<std::shared_ptr<paimon::GlobalIndexReader>>(std::string_view column_name)>;
    using FieldTypeGetter = std::function<StatusOr<std::string>(std::string_view column_name)>;

    PaimonGlobalIndexEvaluator(ReaderGetter reader_getter, FieldTypeGetter field_type_getter)
            : _reader_getter(std::move(reader_getter)), _field_type_getter(std::move(field_type_getter)) {}

    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> evaluate(const rapidjson::Value& node) const;

private:
    StatusOr<std::string> _column_type(const rapidjson::Value& node) const;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> _evaluate_binary(const rapidjson::Value& node) const;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> _evaluate_compound(const rapidjson::Value& node) const;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> _evaluate_in(const rapidjson::Value& node) const;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> _evaluate_is_null(const rapidjson::Value& node) const;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> _evaluate_call(const rapidjson::Value& node) const;

    ReaderGetter _reader_getter;
    FieldTypeGetter _field_type_getter;
};

} // namespace starrocks
