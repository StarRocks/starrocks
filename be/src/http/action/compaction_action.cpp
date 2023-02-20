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

#include "common/logging.h"
#include "common/status.h"
#include "common/tracer.h"
#include "fmt/core.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "runtime/exec_env.h"
#include "storage/base_compaction.h"
#include "storage/compaction_manager.h"
#include "storage/compaction_task.h"
#include "storage/cumulative_compaction.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_updates.h"
#include "util/defer_op.h"
#include "util/json_util.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"

namespace starrocks {

const static std::string HEADER_JSON = "application/json";
const static std::string PARAM_COMPACTION_TYPE = "compaction_type";

std::atomic_bool CompactionAction::_running = false;

// for viewing the compaction status
Status CompactionAction::_handle_show_compaction(HttpRequest* req, std::string* json_result) {
    const std::string& req_tablet_id = req->param(TABLET_ID_KEY);
    const std::string& req_schema_hash = req->param(TABLET_SCHEMA_HASH_KEY);
    if (req_tablet_id == "" && req_schema_hash == "") {
        // TODO(cmy): View the overall compaction status
        return Status::NotSupported("The overall compaction status is not supported yet");
    }

    uint64_t tablet_id;
    try {
        tablet_id = std::stoull(req_tablet_id);
    } catch (const std::exception& e) {
        LOG(WARNING) << "invalid argument.tablet_id:" << req_tablet_id << ", schema_hash:" << req_schema_hash;
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

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    RETURN_IF(tablet == nullptr, Status::InvalidArgument(fmt::format("Not Found tablet:{}", tablet_id)));

    auto* mem_tracker = ExecEnv::GetInstance()->compaction_mem_tracker();
    if (tablet->updates() != nullptr) {
        string rowset_ids_string = req->param("rowset_ids");
        if (rowset_ids_string.empty()) {
            RETURN_IF_ERROR(tablet->updates()->compaction(mem_tracker));
        } else {
            vector<string> id_str_list = strings::Split(rowset_ids_string, ",", strings::SkipEmpty());
            vector<uint32_t> rowset_ids;
            for (const auto& id_str : id_str_list) {
                try {
                    auto rowset_id = std::stoull(id_str);
                    if (rowset_id > UINT32_MAX) {
                        throw std::exception();
                    }
                    rowset_ids.push_back((uint32_t)rowset_id);
                } catch (const std::exception& e) {
                    std::string msg = fmt::format("invalid argument. rowset_ids:{}", rowset_ids_string);
                    LOG(WARNING) << msg;
                    return Status::InvalidArgument(msg);
                }
            }
            if (rowset_ids.empty()) {
                return Status::InvalidArgument(fmt::format("empty argument. rowset_ids:{}", rowset_ids_string));
            }
            RETURN_IF_ERROR(tablet->updates()->compaction(mem_tracker, rowset_ids));
        }
        *json_result = R"({"status": "Success", "msg": "compaction task executed successful"})";
        return Status::OK();
    }

    std::string compaction_type = req->param(PARAM_COMPACTION_TYPE);
    if (compaction_type != to_string(CompactionType::BASE_COMPACTION) &&
        compaction_type != to_string(CompactionType::CUMULATIVE_COMPACTION)) {
        return Status::NotSupported(fmt::format("unsupport compaction type:{}", compaction_type));
    }

    if (compaction_type == to_string(CompactionType::CUMULATIVE_COMPACTION)) {
        StarRocksMetrics::instance()->cumulative_compaction_request_total.increment(1);
        if (config::enable_size_tiered_compaction_strategy) {
            if (tablet->need_compaction()) {
                auto compaction_task = tablet->create_compaction_task();
                if (compaction_task != nullptr) {
                    compaction_task->set_task_id(
                            StorageEngine::instance()->compaction_manager()->next_compaction_task_id());
                    compaction_task->start();
                    if (compaction_task->compaction_task_state() != COMPACTION_SUCCESS) {
                        return Status::InternalError(fmt::format("Failed to base compaction tablet={} err={}",
                                                                 tablet->full_name(),
                                                                 tablet->last_cumu_compaction_failure_status()));
                    }
                }
            }
        } else {
            CumulativeCompaction cumulative_compaction(mem_tracker, tablet);

            Status res = cumulative_compaction.compact();
            if (!res.ok()) {
                if (!res.is_mem_limit_exceeded()) {
                    tablet->set_last_cumu_compaction_failure_time(UnixMillis());
                }
                if (!res.is_not_found()) {
                    StarRocksMetrics::instance()->cumulative_compaction_request_failed.increment(1);
                    LOG(WARNING) << "Fail to vectorized compact tablet=" << tablet->full_name()
                                 << ", err=" << res.to_string();
                }
                return res;
            }
        }
        tablet->set_last_cumu_compaction_failure_time(0);
    } else if (compaction_type == to_string(CompactionType::BASE_COMPACTION)) {
        StarRocksMetrics::instance()->base_compaction_request_total.increment(1);
        if (config::enable_size_tiered_compaction_strategy) {
            if (tablet->force_base_compaction()) {
                auto compaction_task = tablet->create_compaction_task();
                if (compaction_task != nullptr) {
                    compaction_task->set_task_id(
                            StorageEngine::instance()->compaction_manager()->next_compaction_task_id());
                    compaction_task->start();
                    if (compaction_task->compaction_task_state() != COMPACTION_SUCCESS) {
                        return Status::InternalError(fmt::format("Failed to base compaction tablet={} task_id={}",
                                                                 tablet->full_name(), compaction_task->task_id()));
                    }
                }
            } else {
                return Status::InternalError(
                        fmt::format("Failed to base compaction tablet={} no need to do", tablet->full_name()));
            }
        } else {
            BaseCompaction base_compaction(mem_tracker, tablet);

            Status res = base_compaction.compact();
            if (!res.ok()) {
                tablet->set_last_base_compaction_failure_time(UnixMillis());
                if (!res.is_not_found()) {
                    StarRocksMetrics::instance()->base_compaction_request_failed.increment(1);
                    LOG(WARNING) << "failed to init vectorized base compaction. res=" << res.to_string()
                                 << ", tablet=" << tablet->full_name();
                }
                return res;
            }
        }

        tablet->set_last_base_compaction_failure_time(0);
    } else {
        __builtin_unreachable();
    }
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
    root.AddMember("need_to_repair_num", tablets_with_small_segment_files.size(), root.GetAllocator());

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
    root.AddMember("executed_task_num", tasks.size(), root.GetAllocator());

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
            for (const auto& rowset_segments_pair : itr.second) {
                rowsetids.emplace_back(rowset_segments_pair.first);
            }
            tasks.emplace_back(itr.first, std::move(rowsetids));
        }
    }
    StorageEngine::instance()->submit_repair_compaction_tasks(tasks);
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
