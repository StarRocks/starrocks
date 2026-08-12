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

#include "http/action/lake/dump_tablet_metadata_serializer.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/message.h>
#include <json2pb/pb_to_json.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <set>
#include <string_view>
#include <utility>
#include <vector>

#include "gen_cpp/lake_types.pb.h"
#include "http/action/lake/dump_tablet_metadata_serializer_internal.h"

namespace starrocks::lake {
namespace dump_tablet_metadata_internal {

void redact_encryption_metadata(google::protobuf::Message* message, std::set<std::string>* redacted_fields) {
    const auto* reflection = message->GetReflection();
    std::vector<const google::protobuf::FieldDescriptor*> present_fields;
    reflection->ListFields(*message, &present_fields);
    for (const auto* field : present_fields) {
        if (field->name().find("encryption_meta") != std::string::npos) {
            reflection->ClearField(message, field);
            redacted_fields->insert(field->full_name());
            continue;
        }
        if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            continue;
        }
        if (field->is_repeated()) {
            const int count = reflection->FieldSize(*message, field);
            for (int i = 0; i < count; ++i) {
                redact_encryption_metadata(reflection->MutableRepeatedMessage(message, field, i), redacted_fields);
            }
        } else {
            redact_encryption_metadata(reflection->MutableMessage(message, field), redacted_fields);
        }
    }
}

} // namespace dump_tablet_metadata_internal

namespace {

using RedactedFieldNames = std::set<std::string>;

class CappedJsonOutputStream final : public google::protobuf::io::ZeroCopyOutputStream {
public:
    explicit CappedJsonOutputStream(size_t max_bytes) : _max_bytes(max_bytes) {}

    bool Next(void** data, int* size) override {
        _last_grant = 0;
        if (_output.size() >= _max_bytes) {
            _overflowed = true;
            return false;
        }
        constexpr size_t kChunkSize = 4096;
        const size_t remaining = _max_bytes - _output.size();
        const size_t grant = std::min({remaining, kChunkSize, static_cast<size_t>(std::numeric_limits<int>::max())});
        if (grant == 0) {
            _overflowed = true;
            return false;
        }
        const size_t offset = _output.size();
        _output.resize(offset + grant);
        _last_grant = grant;
        *data = _output.data() + offset;
        *size = static_cast<int>(grant);
        return true;
    }

    void BackUp(int count) override {
        if (count < 0 || static_cast<size_t>(count) > _last_grant || static_cast<size_t>(count) > _output.size()) {
            _overflowed = true;
            return;
        }
        _output.resize(_output.size() - count);
        _last_grant = 0;
    }

    int64_t ByteCount() const override { return static_cast<int64_t>(_output.size()); }

    bool Append(std::string_view bytes) {
        if (_output.size() > _max_bytes || bytes.size() > _max_bytes - _output.size()) {
            _overflowed = true;
            return false;
        }
        _output.append(bytes.data(), bytes.size());
        return true;
    }

    bool overflowed() const { return _overflowed; }
    std::string TakeString() { return std::move(_output); }

private:
    std::string _output;
    size_t _max_bytes;
    size_t _last_grant = 0;
    bool _overflowed = false;
};

Status capacity_limit_status() {
    return Status::CapacityLimitExceed("tablet metadata JSON exceeds the diagnostic response limit");
}

} // namespace

StatusOr<DumpTabletMetadataJson> serialize_dump_tablet_metadata(const TabletMetadataPB& metadata,
                                                                size_t max_response_bytes) {
    TabletMetadataPB redacted_metadata = metadata;
    RedactedFieldNames redacted_fields;
    dump_tablet_metadata_internal::redact_encryption_metadata(&redacted_metadata, &redacted_fields);

    CappedJsonOutputStream sink(max_response_bytes);
    if (!sink.Append("{\"metadata\":")) {
        return capacity_limit_status();
    }

    bool converted = false;
    {
        json2pb::Pb2JsonOptions options;
        options.pretty_json = false;
        options.bytes_to_base64 = true;
        converted = json2pb::ProtoMessageToJson(redacted_metadata, &sink, options);
    }
    if (sink.overflowed()) {
        return capacity_limit_status();
    }
    if (!converted) {
        return Status::InternalError("failed to serialize tablet metadata JSON");
    }

    if (!redacted_fields.empty()) {
        if (!sink.Append(",\"redacted_fields\":[")) {
            return capacity_limit_status();
        }
        bool first = true;
        for (const auto& field_name : redacted_fields) {
            if ((!first && !sink.Append(",")) || !sink.Append("\"") || !sink.Append(field_name) || !sink.Append("\"")) {
                return capacity_limit_status();
            }
            first = false;
        }
        if (!sink.Append("]")) {
            return capacity_limit_status();
        }
    }
    if (!sink.Append("}")) {
        return capacity_limit_status();
    }
    if (sink.overflowed()) {
        return capacity_limit_status();
    }

    return DumpTabletMetadataJson{sink.TakeString(), !redacted_fields.empty()};
}

} // namespace starrocks::lake
