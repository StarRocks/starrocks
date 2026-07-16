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

#include <cmath>
#include <cstdint>

#include "storage_primitive/bm25_search_option.h"

namespace starrocks {

// I3: pure BM25 scoring kernel. `idf` is precomputed in Phase-1 (see bm25_idf). Leaf, no I/O.
//
//   score(term) = idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * doc_len / avgdl))
//
inline double bm25_term(uint32_t tf, uint32_t doc_len, double idf, const BM25Stats& s) {
    if (tf == 0) {
        return 0.0;
    }
    const double norm = (s.avgdl > 0.0) ? (static_cast<double>(doc_len) / s.avgdl) : 0.0;
    const double denom = static_cast<double>(tf) + s.k1 * (1.0 - s.b + s.b * norm);
    return idf * (static_cast<double>(tf) * (s.k1 + 1.0)) / denom;
}

// Robertson-Sparck-Jones IDF with the +1 smoothing StarRocks/Doris use (always >= 0):
//   idf = ln(1 + (N - df + 0.5) / (df + 0.5))
inline double bm25_idf(int64_t N, int64_t df) {
    return std::log(1.0 + (static_cast<double>(N) - static_cast<double>(df) + 0.5) / (static_cast<double>(df) + 0.5));
}

} // namespace starrocks
