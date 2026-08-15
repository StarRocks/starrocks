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
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <json2pb/pb_to_json.h>

#include <optional>
#include <set>
#include <vector>

#include "gen_cpp/lake_types.pb.h"
namespace starrocks::lake {
namespace dump_tablet_metadata_internal {

bool contains_encryption_metadata(const google::protobuf::Message& message) {
    const auto* reflection = message.GetReflection();
    std::vector<const google::protobuf::FieldDescriptor*> present_fields;
    reflection->ListFields(message, &present_fields);
    for (const auto* field : present_fields) {
        if (field->name().find("encryption_meta") != std::string::npos) {
            return true;
        }
        if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            continue;
        }
        if (field->is_repeated()) {
            const int count = reflection->FieldSize(message, field);
            for (int i = 0; i < count; ++i) {
                if (contains_encryption_metadata(reflection->GetRepeatedMessage(message, field, i))) {
                    return true;
                }
            }
        } else if (contains_encryption_metadata(reflection->GetMessage(message, field))) {
            return true;
        }
    }
    return false;
}

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

Status capacity_limit_status() {
    return Status::CapacityLimitExceed("tablet metadata JSON exceeds the diagnostic response limit");
}

} // namespace

StatusOr<std::string> serialize_dump_tablet_metadata(const TabletMetadataPB& metadata, size_t max_response_bytes) {
    RedactedFieldNames redacted_fields;
    std::optional<TabletMetadataPB> redacted_metadata;
    const TabletMetadataPB* output_metadata = &metadata;
    if (dump_tablet_metadata_internal::contains_encryption_metadata(metadata)) {
        redacted_metadata.emplace(metadata);
        dump_tablet_metadata_internal::redact_encryption_metadata(&*redacted_metadata, &redacted_fields);
        output_metadata = &*redacted_metadata;
    }

    std::string response = "{\"metadata\":";
    {
        json2pb::Pb2JsonOptions options;
        options.pretty_json = false;
        options.bytes_to_base64 = true;
        google::protobuf::io::StringOutputStream output(&response);
        if (!json2pb::ProtoMessageToJson(*output_metadata, &output, options)) {
            return Status::InternalError("failed to serialize tablet metadata JSON");
        }
    }

    if (!redacted_fields.empty()) {
        response.append(",\"redacted_fields\":[");
        bool first = true;
        for (const auto& field_name : redacted_fields) {
            if (!first) {
                response.push_back(',');
            }
            response.push_back('"');
            response.append(field_name);
            response.push_back('"');
            first = false;
        }
        response.push_back(']');
    }
    response.push_back('}');
    if (response.size() > max_response_bytes) {
        return capacity_limit_status();
    }
    return response;
}

} // namespace starrocks::lake
