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
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/action/compaction_action.cpp

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

#include "http/action/compaction_action.h"

#include <atomic>
#include <sstream>
#include <string>

#include "base/format.h"
#include "base/utility/defer_op.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/tracer.h"
#include "fmt/core.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "storage/compaction_manager.h"
#include "storage/manual_compaction.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "util/json_util.h"

namespace starrocks {

const static std::string HEADER_JSON = "application/json";
const static std::string PARAM_COMPACTION_TYPE = "compaction_type";

std::atomic_bool CompactionAction::_running = false;

// for viewing the compaction status
Status CompactionAction::_handle_show_compaction(HttpRequest* req, std::string* json_result) {
    const std::string& req_tablet_id = req->param(TABLET_ID_KEY);
    if (req_tablet_id == "") {
        std::string msg = fmt::format("The argument 'tablet_id' is required.");
        LOG(WARNING) << msg;
        return Status::NotSupported(msg);
    }

    uint64_t tablet_id;
    try {
        tablet_id = std::stoull(req_tablet_id);
    } catch (const std::exception& e) {
        LOG(WARNING) << "invalid argument.tablet_id:" << req_tablet_id;
        return Status::InternalError(strings::Substitute("convert failed, $0", e.what()));
    }

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::NotFound("Tablet not found");
    }

    tablet->get_compaction_status(json_result);
    return Status::OK();
}

Status get_params(HttpRequest* req, uint64_t* tablet_id) {
    std::string req_tablet_id = req->param(TABLET_ID_KEY);

    try {
        *tablet_id = std::stoull(req_tablet_id);
    } catch (const std::exception& e) {
        std::string msg = fmt::format("invalid argument.tablet_id:{}", req_tablet_id);
        LOG(WARNING) << msg;
        return Status::InvalidArgument(msg);
    }

    return Status::OK();
}

Status CompactionAction::_handle_compaction(HttpRequest* req, std::string* json_result) {
    bool expected = false;
    if (!_running.compare_exchange_strong(expected, true)) {
        return Status::TooManyTasks("Manual compaction task is running");
    }
    DeferOp defer([&]() { _running = false; });
    auto scoped_span = trace::Scope(Tracer::Instance().start_trace("http_handle_compaction"));

    uint64_t tablet_id;
    RETURN_IF_ERROR(get_params(req, &tablet_id));
    std::string compaction_type = req->param(PARAM_COMPACTION_TYPE);
    string rowset_ids_string = req->param("rowset_ids");
    RETURN_IF_ERROR(run_manual_compaction(tablet_id, compaction_type, rowset_ids_string));
    *json_result = R"({"status": "Success", "msg": "compaction task executed successful"})";
    return Status::OK();
}

Status CompactionAction::_handle_show_repairs(HttpRequest* req, std::string* json_result) {
    rapidjson::Document root;
    root.SetObject();

    rapidjson::Document need_to_repair;
    need_to_repair.SetArray();
    auto tablets_with_small_segment_files =
            StorageEngine::instance()->tablet_manager()->get_tablets_need_repair_compaction();
    for (auto& itr : tablets_with_small_segment_files) {
        rapidjson::Document item;
        item.SetObject();
        item.AddMember("tablet_id", itr.first, root.GetAllocator());
        rapidjson::Document rowsets;
        rowsets.SetArray();
        for (const auto& rowset : itr.second) {
            rapidjson::Document rs;
            rs.SetObject();
            rs.AddMember("rowset_id", rowset.first, root.GetAllocator());
            rs.AddMember("segments", rowset.second, root.GetAllocator());
            rowsets.PushBack(rs, root.GetAllocator());
        }
        item.AddMember("rowsets", rowsets, root.GetAllocator());
        need_to_repair.PushBack(item, root.GetAllocator());
    }
    root.AddMember("need_to_repair", need_to_repair, root.GetAllocator());
    root.AddMember("need_to_repair_num", static_cast<uint64_t>(tablets_with_small_segment_files.size()),
                   root.GetAllocator());

    rapidjson::Document executed;
    executed.SetArray();
    auto tasks = StorageEngine::instance()->get_executed_repair_compaction_tasks();
    for (auto& task_info : tasks) {
        rapidjson::Document item;
        item.SetObject();
        item.AddMember("tablet_id", task_info.first, root.GetAllocator());
        rapidjson::Document rowsets;
        rowsets.SetArray();
        for (auto& rowset_st : task_info.second) {
            rapidjson::Document obj;
            obj.SetObject();
            obj.AddMember("rowset_id", rowset_st.first, root.GetAllocator());
            obj.AddMember("status", rapidjson::StringRef(rowset_st.second.c_str()), root.GetAllocator());
            rowsets.PushBack(obj, root.GetAllocator());
        }
        item.AddMember("rowsets", rowsets, root.GetAllocator());
        executed.PushBack(item, root.GetAllocator());
    }
    root.AddMember("executed_task", executed, root.GetAllocator());
    root.AddMember("executed_task_num", static_cast<uint64_t>(tasks.size()), root.GetAllocator());

    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    *json_result = std::string(strbuf.GetString());
    return Status::OK();
}

Status CompactionAction::_handle_submit_repairs(HttpRequest* req, std::string* json_result) {
    auto tablets_with_small_segment_files =
            StorageEngine::instance()->tablet_manager()->get_tablets_need_repair_compaction();
    uint64_t tablet_id;
    vector<pair<int64_t, vector<uint32_t>>> tasks;
    if (get_params(req, &tablet_id).ok()) {
        // do single tablet
        auto itr = tablets_with_small_segment_files.find(tablet_id);
        if (itr == tablets_with_small_segment_files.end()) {
            return Status::NotFound(fmt::format("tablet_id {} not found in repair tablet list", tablet_id));
        }
        vector<uint32_t> rowsetids;
        for (const auto& rowset_segments_pair : itr->second) {
            rowsetids.emplace_back(rowset_segments_pair.first);
        }
        tasks.emplace_back(itr->first, std::move(rowsetids));
    } else {
        // do all tablets
        for (auto& itr : tablets_with_small_segment_files) {
            vector<uint32_t> rowsetids;
            rowsetids.reserve(itr.second.size());
            for (const auto& rowset_segments_pair : itr.second) {
                rowsetids.emplace_back(rowset_segments_pair.first);
            }
            tasks.emplace_back(itr.first, std::move(rowsetids));
        }
    }
    StorageEngine::instance()->submit_repair_compaction_tasks(tasks);
    return Status::OK();
}

Status CompactionAction::_handle_running_task(HttpRequest* req, std::string* json_result) {
    CompactionManager* compaction_manager = StorageEngine::instance()->compaction_manager();
    compaction_manager->get_running_status(json_result);
    return Status::OK();
}

void CompactionAction::handle(HttpRequest* req) {
    LOG(INFO) << req->debug_string();
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    std::string json_result;

    Status st;
    if (_type == CompactionActionType::SHOW_INFO) {
        st = _handle_show_compaction(req, &json_result);
    } else if (_type == CompactionActionType::RUN_COMPACTION) {
        st = _handle_compaction(req, &json_result);
    } else if (_type == CompactionActionType::SHOW_REPAIR) {
        st = _handle_show_repairs(req, &json_result);
    } else if (_type == CompactionActionType::SUBMIT_REPAIR) {
        st = _handle_submit_repairs(req, &json_result);
    } else if (_type == CompactionActionType::SHOW_RUNNING_TASK) {
        st = _handle_running_task(req, &json_result);
    } else {
        st = Status::NotSupported("Action not supported");
    }
    if (!st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::OK, to_json(st));
    } else {
        HttpChannel::send_reply(req, HttpStatus::OK, json_result);
    }
}

} // end namespace starrocks
