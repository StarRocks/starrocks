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

#include "http/action/publish_property_action.h"

#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <charconv>
#include <string>

#include "gutil/strings/substitute.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_headers.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/pk_publish_config.h"
#include "storage/storage_env.h"

namespace starrocks {

namespace {

const char* const kContentTypeJson = "application/json";

const char* const kStatusOk = "OK";
const char* const kStatusNotFound = "NOT_FOUND";
const char* const kStatusInvalidArgument = "INVALID_ARGUMENT";

const char* const kTypePk = "pk";

// Says where the answer would have been, because "not found" on its own reads as "no such tablet".
// A tablet's publish property lives on its loaded primary key index and nowhere else -- it is never
// written down -- so an index that is not in memory leaves nothing to report, and the way to get an
// answer is to publish to that tablet again.
std::string not_in_memory_message(int64_t tablet_id) {
    return strings::Substitute(
            "primary key index of tablet $0 is not in memory; its publish property is held only there and is never "
            "persisted",
            tablet_id);
}

bool parse_tablet_id(const std::string& text, int64_t* tablet_id) {
    const char* begin = text.data();
    const char* end = begin + text.size();
    auto [stop, ec] = std::from_chars(begin, end, *tablet_id);
    return ec == std::errc() && stop == end && *tablet_id > 0;
}

} // namespace

void PublishPropertyAction::handle(HttpRequest* req) {
    const std::string& type = req->param("type");
    if (type.empty()) {
        _reply_error(req, kStatusInvalidArgument, "missing param type");
        return;
    }
    if (type != kTypePk) {
        _reply_error(req, kStatusInvalidArgument,
                     strings::Substitute("invalid param type: '$0', supported: $1", type, kTypePk));
        return;
    }
    _handle_pk(req);
}

void PublishPropertyAction::_handle_pk(HttpRequest* req) {
    const std::string& raw_tablet_id = req->param("tablet_id");
    if (raw_tablet_id.empty()) {
        _reply_error(req, kStatusInvalidArgument, "missing param tablet_id");
        return;
    }
    int64_t tablet_id = 0;
    if (!parse_tablet_id(raw_tablet_id, &tablet_id)) {
        _reply_error(req, kStatusInvalidArgument, strings::Substitute("invalid param tablet_id: '$0'", raw_tablet_id));
        return;
    }

    auto* tablet_mgr = _tablet_manager != nullptr ? _tablet_manager : StorageEnv::GetInstance()->lake_tablet_manager();
    auto* update_mgr = tablet_mgr != nullptr ? tablet_mgr->update_mgr() : nullptr;
    if (update_mgr == nullptr) {
        // A shared-nothing node keeps no lake primary key index at all, which leaves a caller with
        // the same nothing as an index that is not in memory.
        _reply_error(req, kStatusNotFound, not_in_memory_message(tablet_id));
        return;
    }

    PkPublishConfigPtr config = update_mgr->get_publish_config(tablet_id);
    if (config == nullptr) {
        _reply_error(req, kStatusNotFound, not_in_memory_message(tablet_id));
        return;
    }

    _reply(req, kStatusOk, "", [&config](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        root.AddMember("revision", rapidjson::Value(config->revision()), allocator);
        rapidjson::Value properties(rapidjson::kObjectType);
        for (const auto& [name, value] : config->properties()) {
            rapidjson::Value key(name.data(), static_cast<rapidjson::SizeType>(name.size()), allocator);
            properties.AddMember(key, rapidjson::Value(value), allocator);
        }
        root.AddMember("properties", properties, allocator);
    });
}

void PublishPropertyAction::_reply(HttpRequest* req, const std::string& status, const std::string& message,
                                   const std::function<void(rapidjson::Document& root)>& fill) {
    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();
    root.AddMember("status",
                   rapidjson::Value(status.c_str(), static_cast<rapidjson::SizeType>(status.size()), allocator),
                   allocator);
    root.AddMember("message",
                   rapidjson::Value(message.c_str(), static_cast<rapidjson::SizeType>(message.size()), allocator),
                   allocator);
    if (fill) {
        fill(root);
    }

    rapidjson::StringBuffer buffer;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, kContentTypeJson);
    // Always 200: `status` is where a caller reads the verdict, so a second one in the status line
    // would only invite disagreement about which to believe.
    HttpChannel::send_reply(req, HttpStatus::OK, buffer.GetString());
}

void PublishPropertyAction::_reply_error(HttpRequest* req, const std::string& status, const std::string& message) {
    _reply(req, status, message, nullptr);
}

} // namespace starrocks
