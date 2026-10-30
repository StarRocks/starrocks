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

#include "connector/file/file_scan_utils.h"

namespace starrocks {

// Shared predicate for rejected-record logging. Logging is valid only for
// formats whose scanners can return rejected-row detail (CSV / JSON / Parquet /
// ORC / Arrow); both legacy FileScanNode and file connector scan paths call
// this helper to keep the whitelist identical.
Status check_rejected_record_format_support(bool logging_enabled, TFileFormatType::type fmt) {
    if (logging_enabled && fmt != TFileFormatType::FORMAT_CSV_PLAIN && fmt != TFileFormatType::FORMAT_JSON &&
        fmt != TFileFormatType::FORMAT_PARQUET && fmt != TFileFormatType::FORMAT_ORC &&
        fmt != TFileFormatType::FORMAT_ARROW) {
        return Status::InternalError("only support csv/json/parquet/orc/arrow format to log rejected record");
    }
    return Status::OK();
}

} // namespace starrocks
