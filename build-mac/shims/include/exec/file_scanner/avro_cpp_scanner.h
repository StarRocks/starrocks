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

// macOS stub for AvroCppScanner when Avro is disabled
#pragma once

#include "exec/file_scanner/file_scanner.h"

namespace starrocks {

class AvroCppScanner final : public FileScanner {
public:
    AvroCppScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                   ScannerCounter* counter, bool schema_only = false)
            : FileScanner(state, profile, scan_range.params, counter, schema_only), _scan_range(scan_range) {}

    Status open() override { return Status::NotSupported("Avro is disabled on macOS build"); }
    void close() override {}
    StatusOr<ChunkPtr> get_next() override { return Status::NotSupported("Avro is disabled on macOS build"); }
    Status get_schema(std::vector<SlotDescriptor>* /*schema*/) override {
        return Status::NotSupported("Avro is disabled on macOS build");
    }

private:
    const TBrokerScanRange& _scan_range;
};

} // namespace starrocks

