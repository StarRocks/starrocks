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

#include "exprs/builtin_functions.h"
#include "exprs/function_helper.h"

namespace starrocks {

// Vectorized BE-side embedding function. Takes a VARCHAR text column and a JSON config column,
// hits the configured OpenAI-compatible /v1/embeddings endpoint per row (with chunk-level
// async fanout), and returns an ARRAY<FLOAT> column. Used by the semantic-context module to
// move embedding compute off the FE leader so bulk inserts scale across BEs.
class EmbeddingFunctions {
public:
    DEFINE_VECTORIZED_FN(embedding);
};

} // namespace starrocks
