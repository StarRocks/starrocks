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
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

class Column;
class StreamMessageMeta;
struct ByteBuffer;
using ByteBufferPtr = std::shared_ptr<ByteBuffer>;

// Routine-load source-metadata bindings for a scan range, keyed by source slot id. The FE lowers
// metadata functions (kafka_topic(), kafka_header('k'), ...) in the COLUMNS clause into hidden source
// slots and sends a TRoutineLoadMetaColumn per slot; the scanner fills those slots from the message
// metadata by id instead of from the payload, so a payload field of the same name is never shadowed.
using StreamSourceMetaColumns = std::unordered_map<int32_t, TRoutineLoadMetaColumn>;

// Builds the slot-id -> descriptor map from the scan-range descriptor list. Empty for non-routine-load
// and for jobs that use no metadata function.
StreamSourceMetaColumns build_stream_source_meta_columns(const std::vector<TRoutineLoadMetaColumn>& descs);

// Returns the StreamMessageMeta carried by `buf`, or nullptr if the buffer has no Kafka/Pulsar meta
// (e.g. CSV, or a job that references no metadata function).
const StreamMessageMeta* stream_source_meta_of(const ByteBufferPtr& buf);

// Appends one row to `column` from `meta` for a metadata column of the given `kind` (and `key` for a
// HEADER lookup, last-wins). `column` must be a nullable scalar for the scalar kinds, or a nullable
// MAP<VARCHAR,VARCHAR> for HEADERS. Fields absent on the message (null key, unavailable timestamp,
// non-partitioned Pulsar topic, missing header key) are appended as SQL NULL; a null `meta` appends
// NULL for every kind.
Status fill_stream_source_meta_column(TStreamSourceMetaKind::type kind, const std::string& key,
                                      const StreamMessageMeta* meta, Column* column);

} // namespace starrocks
