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

#include <boost/algorithm/string.hpp>
#include <map>
#include <string>

#include "common/status.h"
#include "common/statusor.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/tablet_schema.h"

namespace starrocks {

class TabletSchema;

StatusOr<InvertedImplementType> get_inverted_imp_type(const TabletIndex& tablet_index);

// Returns true when the schema contains at least one Tantivy-backed GIN index.
// This is used by Primary Key write paths to select update modes that preserve
// the segment-row-id to Tantivy-doc-id contract.
bool has_tantivy_index(const TabletSchema& tablet_schema);

std::string inverted_index_parser_type_to_string(InvertedIndexParserType parser_type);

InvertedIndexParserType get_inverted_index_parser_type_from_string(const std::string& parser_str);

std::string get_parser_string_from_properties(const std::map<std::string, std::string>& properties);

std::string get_parser_mode_string_from_properties(const std::map<std::string, std::string>& properties);

StatusOr<std::string> get_tantivy_ngram_tokenizer_name(const std::map<std::string, std::string>& properties);

// Resolve the single Tantivy analyzer configuration carried by TabletIndex.
// New indexes carry canonical AnalyzerSpec JSON; legacy indexes are adapted to
// the historical tokenizer name. Unknown legacy parsers fail closed.
StatusOr<std::string> get_tantivy_analyzer_definition(const std::map<std::string, std::string>& properties);

std::string get_tantivy_analyzer_digest(const std::map<std::string, std::string>& properties);

int32_t get_gram_num_from_properties(const std::map<std::string, std::string>& properties);

bool is_tokenized_from_properties(const std::map<std::string, std::string>& properties);

} // namespace starrocks
