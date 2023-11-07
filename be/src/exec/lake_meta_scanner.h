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

#include <gen_cpp/InternalService_types.h>

#include "common/status.h"
#include "exec/meta_scanner.h"
#include "exec/olap_utils.h"
#include "runtime/runtime_state.h"
#include "storage/lake_meta_reader.h"

#ifdef BE_TEST
// remove final declaration for UT
#define DECL_FINAL
#else
#define DECL_FINAL final
#endif

namespace starrocks {

class LakeMetaScanNode;

class LakeMetaScanner DECL_FINAL : public MetaScanner {
public:
    LakeMetaScanner(LakeMetaScanNode* parent);
    ~LakeMetaScanner() DECL_FINAL = default;

    LakeMetaScanner(const LakeMetaScanner&) = delete;
    LakeMetaScanner(LakeMetaScanner&) = delete;
    void operator=(const LakeMetaScanner&) = delete;
    void operator=(LakeMetaScanner&) = delete;

    Status init(RuntimeState* runtime_state, const MetaScannerParams& params) override;

    Status open(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    Status get_chunk(RuntimeState* state, ChunkPtr* chunk) override;

    bool has_more() override;

protected:
    Status _lazy_init(RuntimeState* runtime_state, const MetaScannerParams& params);
    Status _real_init();

    LakeMetaScanNode* _parent;
    int64_t _tablet_id;
    std::unique_ptr<LakeMetaReader> _reader;
};

} // namespace starrocks

#undef DECL_FINAL
