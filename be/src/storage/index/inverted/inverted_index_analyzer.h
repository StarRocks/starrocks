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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/statusor.h"

namespace lucene {
namespace util {
class Reader;
}
namespace analysis {
class Analyzer;
}
} // namespace lucene

namespace starrocks {

enum class InvertedIndexParserType;
enum class InvertedIndexQueryType;
class GinQueryOptions;

using CharFilterMap = std::map<std::string, std::string>;

class InvertedIndexAnalyzer {
public:
    static StatusOr<std::unique_ptr<lucene::analysis::Analyzer>> create_analyzer(InvertedIndexParserType parser_type);

    static StatusOr<std::vector<std::string>> get_analyse_result(const std::string& search_str,
                                                                 const std::wstring& field_name,
                                                                 const GinQueryOptions* gin_query_options);

    static StatusOr<std::vector<std::string>> get_analyse_result(const std::string& search_str,
                                                                 const std::wstring& field_name,
                                                                 const InvertedIndexParserType& parser_type,
                                                                 const InvertedIndexQueryType& query_type);

private:
    static std::vector<std::string> analyse_result_internal(lucene::util::Reader* reader,
                                                            lucene::analysis::Analyzer* analyzer,
                                                            const std::wstring& field_name,
                                                            const InvertedIndexQueryType& query_type,
                                                            const bool& drop_duplicates = true);
};

} // namespace starrocks
