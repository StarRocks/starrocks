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

#include "http/action/pipeline_blocking_drivers_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <string>

#include "common/logging.h"
#include "exec/pipeline/debug.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"

namespace starrocks {

const static std::string HEADER_JSON = "application/json";
const static std::string ACTION_KEY = "action";
const static std::string ACTION_STAT = "stat";

void PipelineBlockingDriversAction::handle(HttpRequest* req) {
    VLOG_ROW << req->debug_string();
    const auto& action = req->param(ACTION_KEY);
    if (req->method() == HttpMethod::GET) {
        if (action == ACTION_STAT) {
            _handle_stat(req);
        } else {
            _handle_error(req, strings::Substitute("Not support GET method: '$0'", req->uri()));
        }
    } else {
        _handle_error(req,
                      strings::Substitute("Not support $0 method: '$1'", to_method_desc(req->method()), req->uri()));
    }
}

void PipelineBlockingDriversAction::_handle(HttpRequest* req, const std::function<void(rapidjson::Document&)>& func) {
    rapidjson::Document root;
    root.SetObject();
    func(root);
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}

void PipelineBlockingDriversAction::_handle_stat(HttpRequest* req) {
    using namespace starrocks::pipeline::debug;
    _handle(req, [=](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();

        QueryMap query_map_not_in_wg;
        _exec_env->wg_driver_executor()->iterate_immutable_blocking_driver(DriverListAggregator()(query_map_not_in_wg));
        rapidjson::Document queries_not_in_wg_obj = DumpQueryMapToJson(allocator)(query_map_not_in_wg);

        QueryMap query_map_in_wg;
        _exec_env->wg_driver_executor()->iterate_immutable_blocking_driver(DriverListAggregator()(query_map_in_wg));
        rapidjson::Document queries_in_wg_obj = DumpQueryMapToJson(allocator)(query_map_in_wg);

        root.AddMember("queries_not_in_workgroup", queries_not_in_wg_obj, allocator);
        root.AddMember("queries_in_workgroup", queries_in_wg_obj, allocator);
    });
}

void PipelineBlockingDriversAction::_handle_error(HttpRequest* req, const std::string& err_msg) {
    _handle(req, [err_msg](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        root.AddMember("error", rapidjson::Value(err_msg.c_str(), err_msg.size()), allocator);
    });
}

} // namespace starrocks
