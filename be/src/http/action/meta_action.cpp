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
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/action/meta_action.cpp

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

#include "http/action/meta_action.h"

#include <sstream>
#include <string>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta.h"
#include "util/json_util.h"

namespace starrocks {

const static std::string HEADER_JSON = "application/json";

Status MetaAction::_handle_header(HttpRequest* req, std::string* json_meta) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    std::string req_tablet_id = req->param(TABLET_ID_KEY);
    uint64_t tablet_id = 0;
    try {
        tablet_id = std::stoull(req_tablet_id);
    } catch (const std::exception& e) {
        LOG(WARNING) << "invalid argument.tablet_id:" << req_tablet_id;
        return Status::InternalError(strings::Substitute("convert failed, $0", e.what()));
    }

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "no tablet for tablet_id:" << tablet_id;
        return Status::InternalError("no tablet exist");
    }
    auto tablet_meta = TabletMeta::create();
    tablet->generate_tablet_meta_copy(tablet_meta);
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    tablet_meta->to_json(json_meta, json_options);
    return Status::OK();
}

void MetaAction::handle(HttpRequest* req) {
    if (_meta_type == META_TYPE::HEADER) {
        std::string json_meta;
        Status status = _handle_header(req, &json_meta);
        std::string status_result = to_json(status);
        LOG(INFO) << "handle request result:" << status_result;
        if (status.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, json_meta);
        } else {
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status_result);
        }
    }
}

} // end namespace starrocks
