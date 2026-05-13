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

#include "exec/hdfs_scanner/hdfs_scanner.h"

namespace starrocks {

/**
 * Single-round Paimon vector data scanner.
 * Performs both ANN search and data retrieval against a Paimon global vector index shard.
 *
 * The FE passes a TPaimonVectorSearchCondition per-shard through THdfsScanRange.
 * This scanner uses the condition to execute the ANN search via the Paimon native index,
 * then fetches the corresponding rows and produces output chunks.
 *
 * Lifecycle:
 *   do_init  -> parse vector search condition from scan range
 *   do_open  -> open the Paimon native index and execute ANN search
 *   do_get_next -> iterate over result rows and fill chunks
 *   do_close -> release resources
 */
class PaimonVectorDataScanner : public HdfsScanner {
public:
    PaimonVectorDataScanner() = default;
    ~PaimonVectorDataScanner() override = default;

    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    Status do_open(RuntimeState* state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;

private:
    const TPaimonVectorSearchCondition* _vector_condition = nullptr;
    bool _is_finished = false;
};

} // namespace starrocks
