// This file is made available under Elastic License 2.0.
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
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "runtime/exec_env.h"
#include "storage/base_compaction.h"
#include "storage/cumulative_compaction.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "util/defer_op.h"
#include "util/json_util.h"
#include "util/runtime_profile.h"
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
    if (_running) {
        return Status::TooManyTasks("Manual compaction task is running");
    }
    _running = true;
    auto scoped_span = trace::Scope(Tracer::Instance().start_trace("http_handle_compaction"));
    DeferOp defer([&]() { _running = false; });

    uint64_t tablet_id;
    RETURN_IF_ERROR(get_params(req, &tablet_id));

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    RETURN_IF(tablet == nullptr,
              Status::InvalidArgument(fmt::format("Not Found tablet:{}, schema hash:{}", tablet_id)));

    std::string compaction_type = req->param(PARAM_COMPACTION_TYPE);
    if (compaction_type != to_string(CompactionType::BASE_COMPACTION) &&
        compaction_type != to_string(CompactionType::CUMULATIVE_COMPACTION)) {
        return Status::NotSupported(fmt::format("unsupport compaction type:{}", compaction_type));
    }

    StarRocksMetrics::instance()->cumulative_compaction_request_total.increment(1);
    auto* mem_tracker = ExecEnv::GetInstance()->compaction_mem_tracker();

    if (compaction_type == to_string(CompactionType::CUMULATIVE_COMPACTION)) {
        vectorized::CumulativeCompaction cumulative_compaction(mem_tracker, tablet);

        Status res = cumulative_compaction.compact();
        if (!res.ok()) {
            if (!res.is_mem_limit_exceeded()) {
                tablet->set_last_cumu_compaction_failure_time(UnixMillis());
            }
            if (!res.is_not_found()) {
                StarRocksMetrics::instance()->cumulative_compaction_request_failed.increment(1);
                LOG(WARNING) << "Fail to vectorized compact table=" << tablet->full_name()
                             << ", err=" << res.to_string();
            }
            return res;
        }
        tablet->set_last_cumu_compaction_failure_time(0);
    } else if (compaction_type == to_string(CompactionType::BASE_COMPACTION)) {
        vectorized::BaseCompaction base_compaction(mem_tracker, tablet);

        Status res = base_compaction.compact();
        if (!res.ok()) {
            tablet->set_last_base_compaction_failure_time(UnixMillis());
            if (!res.is_not_found()) {
                StarRocksMetrics::instance()->base_compaction_request_failed.increment(1);
                LOG(WARNING) << "failed to init vectorized base compaction. res=" << res.to_string()
                             << ", table=" << tablet->full_name();
            }
            return res;
        }

        tablet->set_last_base_compaction_failure_time(0);
    } else {
        __builtin_unreachable();
    }
    _running = false;
    *json_result = R"({"status": "Success", "msg": "compaction task executed successful"})";
    return Status::OK();
}

void CompactionAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    std::string json_result;

    if (_type == CompactionActionType::SHOW_INFO) {
        Status st = _handle_show_compaction(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, to_json(st));
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    } else if (_type == CompactionActionType::RUN_COMPACTION) {
        Status st = _handle_compaction(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, to_json(st));
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    } else {
        HttpChannel::send_reply(req, HttpStatus::OK, to_json(Status::NotSupported("Action not supported")));
    }
}

} // end namespace starrocks
