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

void add_enable_phrase_query_sequential_opt_options(TabletIndex* tablet_index, bool enable_phrase_query_sequential_opt);
bool is_enable_phrase_query_sequential_opt(const TabletIndex& tablet_index);

void add_gin_max_expansions(TabletIndex* tablet_index, int32_t gin_max_expansions);
int32_t get_gin_max_expansions(const TabletIndex& tablet_index);

StatusOr<InvertedImplementType> get_inverted_imp_type(const TabletIndex& tablet_index);

std::string inverted_index_parser_type_to_string(InvertedIndexParserType parser_type);

InvertedIndexParserType get_inverted_index_parser_type_from_string(const std::string& parser_str);
InvertedParserMode get_inverted_index_parser_mode_from_string(const std::string& parser_mode_str);

std::string get_parser_string_from_properties(const std::map<std::string, std::string>& properties);

InvertedParserMode get_parser_mode_from_properties(const std::map<std::string, std::string>& properties);

std::string get_support_phrase_from_properties(const std::map<std::string, std::string>& properties);

std::map<std::string, std::string> get_parser_char_filter_map_from_properties(
        const std::map<std::string, std::string>& properties);

int32_t get_ignore_above_from_properties(const std::map<std::string, std::string>& properties);

bool get_lower_case_from_properties(const std::map<std::string, std::string>& properties);

std::string get_stop_words_from_properties(const std::map<std::string, std::string>& properties);

bool is_tokenized_from_properties(const std::map<std::string, std::string>& properties);

} // namespace starrocks