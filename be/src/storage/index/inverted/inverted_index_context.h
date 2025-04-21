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

#include <memory>

#include "inverted_index_common.h"

namespace lucene::analysis {
class Analyzer;
}

namespace starrocks {

class InvertedIndexCtx {
public:
    InvertedIndexCtx() = default;
    InvertedIndexCtx(InvertedIndexQueryType _query_type, InvertedIndexReaderType _reader_type,
                     InvertedIndexParserType _parser_type, std::unique_ptr<lucene::analysis::Analyzer> _analyzer)
            : query_type(_query_type),
              reader_type(_reader_type),
              parser_type(_parser_type),
              analyzer(std::move(_analyzer)) {};

    void setQueryType(InvertedIndexQueryType _query_type) { query_type = _query_type; }
    void setReaderType(InvertedIndexReaderType _reader_type) { reader_type = _reader_type; }
    void setParserType(InvertedIndexParserType _parser_type) { parser_type = _parser_type; }
    void setAnalyzer(std::unique_ptr<lucene::analysis::Analyzer> _analyzer) { analyzer = std::move(_analyzer); }

    InvertedIndexQueryType getQueryType() const { return query_type; }
    InvertedIndexReaderType getReaderType() const { return reader_type; }
    InvertedIndexParserType getParserType() const { return parser_type; }
    std::unique_ptr<lucene::analysis::Analyzer>& getAnalyzer() { return analyzer; }

private:
    InvertedIndexQueryType query_type = InvertedIndexQueryType::UNKNOWN_QUERY;
    InvertedIndexReaderType reader_type = InvertedIndexReaderType::UNKNOWN;
    InvertedIndexParserType parser_type = InvertedIndexParserType::PARSER_UNKNOWN;

    std::unique_ptr<lucene::analysis::Analyzer> analyzer = nullptr;
};

} // namespace starrocks
