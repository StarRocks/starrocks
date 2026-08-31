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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/action/update_config_action.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/action/update_config_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <string>

#include "common/config_http_fwd.h"
#include "common/config_update_registry.h"
#include "common/logging.h"
#include "common/system/master_info.h"
#include "gutil/strings/substitute.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_headers.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"

namespace starrocks {

const static std::string HEADER_JSON = "application/json";

namespace {
// Mutating BE runtime config via this endpoint had no caller authentication at all: any
// request that reached the BE HTTP port could rewrite arbitrary config. Gate it behind the
// same cluster-internal shared token used by other BE-internal HTTP admin endpoints.
Status check_internal_token(HttpRequest* req) {
    const std::string& token_str = req->param("token");
    if (token_str.empty()) {
        return Status::InternalError("token is not specified.");
    }
    if (token_str != get_master_token()) {
        return Status::InternalError("invalid token.");
    }
    return Status::OK();
}
} // namespace

void UpdateConfigAction::handle(HttpRequest* req) {
    LOG(INFO) << req->debug_string();

    if (config::enable_token_check) {
        Status token_st = check_internal_token(req);
        if (!token_st.ok()) {
            LOG(WARNING) << "Rejected update_config request: " << token_st;
            HttpChannel::send_reply(req, HttpStatus::UNAUTHORIZED, std::string(token_st.message()));
            return;
        }
    }

    Status s;
    std::string msg;
    auto* params = req->params();
    // 'token' is an auth parameter consumed above, not a config to set; exclude it when
    // checking that exactly one config_name=new_value pair was supplied.
    size_t config_param_count = params->count("token") > 0 ? params->size() - 1 : params->size();
    if (config_param_count != 1) {
        s = Status::InvalidArgument("");
        msg = "Now only support to set a single config once, via 'config_name=new_value'";
    } else {
        std::string config;
        std::string new_value;
        for (auto& [key, value] : *params) {
            if (key != "token") {
                config = key;
                new_value = value;
                break;
            }
        }
        s = ConfigUpdateRegistry::instance()->update_config(config, new_value);
        if (!s.ok()) {
            LOG(WARNING) << "set_config " << config << "=" << new_value << " failed";
            msg = strings::Substitute("set $0=$1 failed, reason: $2", config, new_value, s.to_string());
        }
    }

    std::string status(s.ok() ? "OK" : "BAD");
    rapidjson::Document root;
    root.SetObject();
    root.AddMember("status", rapidjson::Value(status.c_str(), status.size()), root.GetAllocator());
    root.AddMember("msg", rapidjson::Value(msg.c_str(), msg.size()), root.GetAllocator());
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}

} // namespace starrocks
