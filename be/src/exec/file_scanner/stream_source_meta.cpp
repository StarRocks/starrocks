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

#include "exec/file_scanner/stream_source_meta.h"

#include <fmt/format.h>

#include "base/string/slice.h"
#include "column/column.h"
#include "runtime/byte_buffer.h"
#include "types/datum.h"

namespace starrocks {

StreamSourceMetaColumns build_stream_source_meta_columns(const std::vector<TRoutineLoadMetaColumn>& descs) {
    StreamSourceMetaColumns columns;
    for (const auto& desc : descs) {
        if (desc.__isset.slot_id && desc.__isset.kind) {
            columns.emplace(desc.slot_id, desc);
        }
    }
    return columns;
}

const StreamMessageMeta* stream_source_meta_of(const ByteBufferPtr& buf) {
    if (buf == nullptr) {
        return nullptr;
    }
    auto type = buf->meta()->type();
    if (type == ByteBufferMetaType::KAFKA || type == ByteBufferMetaType::PULSAR) {
        return static_cast<const StreamMessageMeta*>(buf->meta());
    }
    return nullptr;
}

Status fill_stream_source_meta_column(TStreamSourceMetaKind::type kind, const StreamMessageMeta* meta, Column* column) {
    if (meta == nullptr) {
        column->append_nulls(1);
        return Status::OK();
    }

    switch (kind) {
    case TStreamSourceMetaKind::TOPIC:
        if (meta->topic().empty()) {
            column->append_nulls(1);
        } else {
            column->append_datum(Datum(Slice(meta->topic())));
        }
        break;
    case TStreamSourceMetaKind::PARTITION:
        if (meta->partition() < 0) {
            column->append_nulls(1);
        } else {
            column->append_datum(Datum(static_cast<int32_t>(meta->partition())));
        }
        break;
    case TStreamSourceMetaKind::OFFSET:
        if (meta->offset() < 0) {
            column->append_nulls(1);
        } else {
            column->append_datum(Datum(static_cast<int64_t>(meta->offset())));
        }
        break;
    case TStreamSourceMetaKind::MESSAGE_ID:
        if (meta->message_id().empty()) {
            column->append_nulls(1);
        } else {
            column->append_datum(Datum(Slice(meta->message_id())));
        }
        break;
    case TStreamSourceMetaKind::TIMESTAMP:
        if (meta->timestamp() < 0) {
            column->append_nulls(1);
        } else {
            column->append_datum(Datum(static_cast<int64_t>(meta->timestamp())));
        }
        break;
    case TStreamSourceMetaKind::EVENT_TIME:
        if (meta->event_timestamp() < 0) {
            column->append_nulls(1);
        } else {
            column->append_datum(Datum(static_cast<int64_t>(meta->event_timestamp())));
        }
        break;
    case TStreamSourceMetaKind::KEY:
        if (!meta->has_key()) {
            column->append_nulls(1);
        } else {
            column->append_datum(Datum(Slice(meta->key())));
        }
        break;
    case TStreamSourceMetaKind::HEADERS: {
        // MAP<VARCHAR,VARCHAR>; duplicate keys collapse last-wins (DatumMap assignment overwrites).
        // Header values are raw bytes placed into VARCHAR as-is (no UTF-8 validation). Always present
        // (possibly empty), so this column is never NULL.
        DatumMap datum_map;
        for (const auto& kv : meta->headers()) {
            datum_map[Slice(kv.first)] = Datum(Slice(kv.second));
        }
        column->append_datum(Datum(std::move(datum_map)));
        break;
    }
    default:
        return Status::InternalError(fmt::format("unknown stream source meta kind: {}", static_cast<int>(kind)));
    }
    return Status::OK();
}

} // namespace starrocks
