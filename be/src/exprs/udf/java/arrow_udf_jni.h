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

#include <arrow/c/abi.h>

#include "column/column.h"
#include "common/status.h"
#include "common/statusor.h"
#include "jni.h"
#include "runtime/java/java_global_ref.h"

namespace arrow {
class RecordBatch;
}

namespace starrocks {
struct TypeDescriptor;

// Neutral low-level helpers for the vectorized ("input"="arrow") Java UDF paths, shared by the
// scalar / UDTF / UDAF implementations but living in their own translation unit so none of those
// paths depend on each other.

// RAII holder for a StarRocks column batch exported to the Arrow C Data Interface. Exporting fills
// the owned ArrowArray/ArrowSchema C structs with a release callback; the destructor invokes those
// callbacks unless the Java importer already consumed (moved) the structs — importVectorSchemaRoot
// nulls the source release on success, so this is a no-op on the happy path and a leak-guard on
// error. Move/copy are disabled: the struct addresses handed to JNI must stay stable, so it is only
// ever used as a local.
class ExportedArrowBatch {
public:
    ExportedArrowBatch() = default;
    ~ExportedArrowBatch();
    ExportedArrowBatch(const ExportedArrowBatch&) = delete;
    ExportedArrowBatch& operator=(const ExportedArrowBatch&) = delete;

    // Export an already-built RecordBatch (scalar path: the base class builds it).
    Status export_record_batch(const arrow::RecordBatch& batch);
    // Build a RecordBatch from StarRocks columns and export it (UDTF path).
    Status export_columns(size_t num_rows, const Columns& columns, const TypeDescriptor* arg_types);
    // Same, from raw column pointers (UDAF path, where the agg framework passes const Column**).
    Status export_columns(size_t num_rows, const Column* const* columns, size_t num_cols,
                          const TypeDescriptor* arg_types);

    jlong schema_addr() { return reinterpret_cast<jlong>(&_schema); }
    jlong array_addr() { return reinterpret_cast<jlong>(&_array); }

private:
    ArrowArray _array{};
    ArrowSchema _schema{};
};

// The process-global com.starrocks.udf.ArrowUDFHelper class, resolved and cached on first use
// (thread-safe). Returned as a borrowed global jclass owned by the cache. Callers then
// GetStaticMethodID the entry point they need (evaluateArrow / evaluateUDTF / batchUpdateSingle) —
// GetStaticMethodID is cheap and jmethodIDs are process-stable, so per-batch resolution is fine.
// Must be called on a JNI-capable thread.
StatusOr<jclass> arrow_udf_helper_class();

} // namespace starrocks
