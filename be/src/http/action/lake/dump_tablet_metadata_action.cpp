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

#include "http/action/lake/dump_tablet_metadata_action.h"

#include <event2/buffer.h>
#include <event2/http.h>
#include <json2pb/pb_to_json.h>

#include "fs/fs.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "http/http_stream_channel.h"
#include "runtime/exec_env.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "util/string_parser.hpp"

namespace starrocks::lake {

static const char* const kParamPretty = "pretty";

void DumpTabletMetadataAction::handle(HttpRequest* req) {
    std::string tablet_id_str = req->param("TabletId");
    StringParser::ParseResult result;
    auto tablet_id = StringParser::string_to_int<int64_t>(tablet_id_str.data(), tablet_id_str.size(), &result);
    if (result != StringParser::PARSE_SUCCESS) {
        HttpChannel::send_error(req, HttpStatus::BAD_REQUEST);
        return;
    }
    const auto& pretty_str = req->param(kParamPretty);
    bool pretty = true;
    if (!pretty_str.empty()) {
        pretty = StringParser::string_to_bool(pretty_str.data(), pretty_str.size(), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            HttpChannel::send_error(req, HttpStatus::BAD_REQUEST);
            return;
        }
    }

    TabletManager* tablet_mgr = _exec_env->lake_tablet_manager();
    if (tablet_mgr == nullptr) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, "Not built with --use-staros");
        return;
    }

    auto location = tablet_mgr->location_provider()->metadata_root_location(tablet_id);
    auto fs_or = FileSystem::CreateSharedFromString(location);
    if (!fs_or.ok()) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, fs_or.status().to_string());
        return;
    }
    auto fs = std::move(fs_or).value();

    HttpStreamChannel response(req);
    response.start();
    response.write("[\n");
    bool first_object = true;
    auto st = fs->iterate_dir(location, [&](std::string_view name) {
        if (is_tablet_metadata(name)) {
            if (!first_object) {
                response.write(",\n");
            } else {
                first_object = false;
            }
            auto path = join_path(location, name);
            auto metadata_or = tablet_mgr->get_tablet_metadata(path, false);
            if (!metadata_or.ok() && !metadata_or.status().is_not_found()) {
                response.write(R"({"error": ")").write(metadata_or.status().to_string()).write("\"}");
            } else if (metadata_or.ok()) {
                auto metadata = std::move(metadata_or).value();
                json2pb::Pb2JsonOptions options;
                options.pretty_json = pretty;
                std::string json;
                std::string error;
                if (!json2pb::ProtoMessageToJson(*metadata, &json, options, &error)) {
                    response.write(R"({"error": ")").write(error).write("\"}");
                } else {
                    response.write(json);
                }
            }
        }
        return true;
    });

    if (!st.ok()) {
        response.write(R"({"error": ")").write(st.to_string()).write("\"}");
    }

    response.write("\n]\n");
}

} // namespace starrocks::lake
