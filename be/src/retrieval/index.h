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

#include <cstdint>

#include "common/status.h"

namespace starrocks {

// Category of a retrieval index. Lets a heterogeneous index cache/registry
// hold vector and full-text indexes behind a single BaseIndex handle.
enum class IndexType {
    VECTOR,
    FULLTEXT,
};

// ============================================================
// BaseIndex — query-agnostic lifecycle surface shared by every
// retrieval index modality (vector ANN, full-text, ...).
//
// Intentionally thin. The query type is the one axis that genuinely
// differs between modalities, so init()/search()/make_iterator()
// live on the modality-specific subclass (VectorAnnIndex, future
// FullTextIndex). What is truly shared lives here (lifecycle) and in
// the query-agnostic primitives row_id_filter.h / scored_result.h.
//
// The base handle's value is letting an index cache hold heterogeneous
// indexes uniformly: evict by mem_usage(), release by close(), and
// branch on type() to recover the concrete modality.
// ============================================================
class BaseIndex {
public:
    virtual ~BaseIndex() = default;

    virtual Status close() = 0;

    virtual int64_t mem_usage() const = 0;

    virtual IndexType type() const = 0;
};

} // namespace starrocks
