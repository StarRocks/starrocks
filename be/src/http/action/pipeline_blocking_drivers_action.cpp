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

struct DriverInfo {
    int32_t driver_id;
    pipeline::DriverState state;
    std::string driver_desc;
    bool is_fragment_cancelled;
    std::string fragment_status;

    DriverInfo(int32_t driver_id, pipeline::DriverState state, std::string&& driver_desc, bool cancelled,
               std::string status)
            : driver_id(driver_id),
              state(state),
              driver_desc(std::move(driver_desc)),
              is_fragment_cancelled(cancelled),
              fragment_status(std::move(status)) {}
};

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
    _handle(req, [=](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();

        using DriverInfoList = std::vector<DriverInfo>;
        using FragmentMap = std::unordered_map<TUniqueId, DriverInfoList>;
        using QueryMap = std::unordered_map<TUniqueId, FragmentMap>;

        auto query_map_to_doc_func = [&allocator](const QueryMap& query_map) {
            rapidjson::Document queries_obj;
            queries_obj.SetArray();
            for (const auto& [query_id, fragment_map] : query_map) {
                rapidjson::Document fragments_obj;
                fragments_obj.SetArray();
                for (const auto& [fragment_id, driver_info_list] : fragment_map) {
                    rapidjson::Document drivers_obj;
                    drivers_obj.SetArray();
                    bool is_fragment_cancelled = false;
                    std::string status;
                    for (const auto& driver_info : driver_info_list) {
                        rapidjson::Document driver_obj;
                        driver_obj.SetObject();

                        driver_obj.AddMember("driver_id", rapidjson::Value(driver_info.driver_id), allocator);
                        driver_obj.AddMember("state",
                                             rapidjson::Value(ds_to_string(driver_info.state).c_str(), allocator),
                                             allocator);
                        driver_obj.AddMember("driver_desc",
                                             rapidjson::Value(driver_info.driver_desc.c_str(), allocator), allocator);

                        drivers_obj.PushBack(driver_obj, allocator);
                        if (driver_info.is_fragment_cancelled) {
                            is_fragment_cancelled = true;
                        }
                        status = std::move(driver_info.fragment_status);
                    }

                    rapidjson::Document fragment_obj;
                    fragment_obj.SetObject();
                    fragment_obj.AddMember("fragment_id", rapidjson::Value(print_id(fragment_id).c_str(), allocator),
                                           allocator);
                    fragment_obj.AddMember(
                            "fragment_status",
                            rapidjson::Value((status + (is_fragment_cancelled ? ", cancelled" : "")).c_str(),
                                             allocator),
                            allocator);
                    fragment_obj.AddMember("drivers", drivers_obj, allocator);
                    fragments_obj.PushBack(fragment_obj, allocator);
                }

                rapidjson::Document query_obj;
                query_obj.SetObject();
                query_obj.AddMember("query_id", rapidjson::Value(print_id(query_id).c_str(), allocator), allocator);
                query_obj.AddMember("fragments", fragments_obj, allocator);
                queries_obj.PushBack(query_obj, allocator);
            }

            return queries_obj;
        };

        auto iterate_func_generator = [](QueryMap& query_map) {
            return [&query_map](pipeline::DriverConstRawPtr driver) {
                TUniqueId query_id = driver->query_ctx()->query_id();
                TUniqueId fragment_id = driver->fragment_ctx()->fragment_instance_id();
                bool is_cancelled = driver->fragment_ctx()->is_canceled();
                std::string status = driver->fragment_ctx()->final_status().to_string();
                int32_t driver_id = driver->driver_id();
                pipeline::DriverState state = driver->driver_state();
                std::string driver_desc = driver->to_readable_string();

                auto fragment_map_it = query_map.find(query_id);
                if (fragment_map_it == query_map.end()) {
                    fragment_map_it = query_map.emplace(query_id, FragmentMap()).first;
                }
                auto& fragment_map = fragment_map_it->second;
                auto driver_list_it = fragment_map.find(fragment_id);
                if (driver_list_it == fragment_map.end()) {
                    driver_list_it = fragment_map.emplace(fragment_id, DriverInfoList()).first;
                }
                driver_list_it->second.emplace_back(driver_id, state, std::move(driver_desc), is_cancelled, status);
            };
        };

        QueryMap query_map_not_in_wg;
        _exec_env->wg_driver_executor()->iterate_immutable_blocking_driver(iterate_func_generator(query_map_not_in_wg));
        rapidjson::Document queries_not_in_wg_obj = query_map_to_doc_func(query_map_not_in_wg);

        QueryMap query_map_in_wg;
        _exec_env->wg_driver_executor()->iterate_immutable_blocking_driver(iterate_func_generator(query_map_in_wg));
        rapidjson::Document queries_in_wg_obj = query_map_to_doc_func(query_map_in_wg);

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
