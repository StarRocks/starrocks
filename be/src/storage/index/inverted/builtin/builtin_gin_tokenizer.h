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

#include <string>
#include <vector>

#include "base/string/slice.h"
#include "common/status.h"
#include "storage/index/inverted/inverted_index_common.h"

namespace starrocks {

// Tokenize a query string exactly as the builtin GIN index tokenizes on write, per the index's parser
// (none / english / standard / chinese). Single source of truth for MATCH filtering and BM25 Phase-1/2,
// so all resolve identical terms. Builds a throw-away analyzer per call (per-query, so cheap).
//
// Declared in this lightweight header (no CLucene) so callers that only need the tokenizer -- e.g. BM25
// Phase-1 -- don't have to pull in the CLucene-heavy builtin_inverted_index_iterator.h.
Status tokenize_builtin_gin_query(InvertedIndexParserType parser_type, bool lower_case, const Slice& query,
                                  std::vector<std::string>* tokens);

} // namespace starrocks
