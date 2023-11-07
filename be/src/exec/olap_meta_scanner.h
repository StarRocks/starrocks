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
#include "storage/olap_meta_reader.h"

namespace starrocks {

class OlapMetaScanNode;

class OlapMetaScanner final : public MetaScanner {
public:
    OlapMetaScanner(OlapMetaScanNode* parent);
    ~OlapMetaScanner() override = default;

    OlapMetaScanner(const OlapMetaScanner&) = delete;
    OlapMetaScanner(OlapMetaScanner&) = delete;
    void operator=(const OlapMetaScanner&) = delete;
    void operator=(OlapMetaScanner&) = delete;

    Status init(RuntimeState* runtime_state, const MetaScannerParams& params) override;

    Status open(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    Status get_chunk(RuntimeState* state, ChunkPtr* chunk) override;

    bool has_more() override;

private:
    Status _get_tablet(const TInternalScanRange* scan_range);
    Status _init_meta_reader_params();

    OlapMetaScanNode* _parent;
    TabletSharedPtr _tablet;

    OlapMetaReaderParams _reader_params;
    std::shared_ptr<OlapMetaReader> _reader;
};

} // namespace starrocks
