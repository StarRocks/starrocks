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

#include "connector/hive/scanner/hdfs_scanner.h"

namespace starrocks {

class PaimonNativeReader;

// Integrates PaimonNativeReader with the HDFS scanner lifecycle and applies
// StarRocks side columns and residual predicates to materialized chunks.
class PaimonScanner final : public HdfsScanner {
public:
    PaimonScanner() = default;
    ~PaimonScanner() override;

    Status do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) override;
    Status do_open(RuntimeState* runtime_state) override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    void do_prepare_close() noexcept override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    void do_update_counter(HdfsScannerProfile* profile) override;

private:
    std::unique_ptr<PaimonNativeReader> _reader;
};

} // namespace starrocks
