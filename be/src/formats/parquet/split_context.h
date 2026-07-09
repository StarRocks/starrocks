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

#include "compute_env/query/file_scan_split_context.h"
#include "formats/parquet/metadata.h"
#include "formats/scan_context.h"

namespace starrocks::parquet {

struct SplitContext : public FileScanSplitContext {
    FileMetaDataPtr file_metadata;
    SkipRowsContextPtr skip_rows_ctx;

    FileScanSplitContextPtr clone() override {
        auto ctx = std::make_unique<SplitContext>();
        ctx->file_metadata = file_metadata;
        ctx->skip_rows_ctx = skip_rows_ctx;
        return ctx;
    }
};

} // namespace starrocks::parquet
