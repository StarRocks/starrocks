// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#include "http/action/list_workgroup_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <string>

#include "common/logging.h"
#include "exec/workgroup/work_group.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"

namespace starrocks {

const static std::string HEADER_JSON = "application/json";

static inline std::string workgroup_type_to_string(TWorkGroupType::type type) {
    switch (type) {
    case TWorkGroupType::WG_DEFAULT:
        return "default";
    case TWorkGroupType::WG_NORMAL:
        return "normal";
    case TWorkGroupType::WG_REALTIME:
        return "realtime";
    default:
        return "unknown";
    }
}

void ListWorkGroupAction::handle(HttpRequest* req) {
    LOG(INFO) << req->debug_string();
    auto workgroups = workgroup::WorkGroupManager::instance()->list_all_workgroups();
    rapidjson::Document root;
    root.SetArray();
    auto& allocator = root.GetAllocator();
    for (const auto& wg : workgroups) {
        rapidjson::Document wg_item;
        wg_item.SetObject();
        wg_item.AddMember("name", rapidjson::Value(wg.name.c_str(), wg.name.size()), allocator);
        wg_item.AddMember("id", rapidjson::Value(wg.id), allocator);
        wg_item.AddMember("version", rapidjson::Value(wg.version), allocator);
        std::string workgroup_type = workgroup_type_to_string(wg.workgroup_type);
        rapidjson::Value type;
        type.SetString(workgroup_type.c_str(), workgroup_type.size(), allocator);
        wg_item.AddMember("type", type, allocator);
        wg_item.AddMember("state", rapidjson::Value(wg.state.c_str(), wg.state.size()), allocator);

        int64_t mem_bytes_limit = _exec_env->query_pool_mem_tracker()->limit() * wg.mem_limit;
        wg_item.AddMember("cpu_core_limit", rapidjson::Value(wg.cpu_core_limit), allocator);
        wg_item.AddMember("mem_limit", rapidjson::Value(wg.mem_limit), allocator);
        wg_item.AddMember("mem_bytes_limit", rapidjson::Value(mem_bytes_limit), allocator);
        wg_item.AddMember("concurrency_limit", rapidjson::Value(wg.concurrency_limit), allocator);
        wg_item.AddMember("num_drivers", rapidjson::Value(wg.num_drivers), allocator);
        wg_item.AddMember("big_query_cpu_core_second_limit", rapidjson::Value(wg.big_query_cpu_core_second_limit),
                          allocator);
        wg_item.AddMember("big_query_mem_list", rapidjson::Value(wg.big_query_mem_limit), allocator);
        wg_item.AddMember("big_query_scan_rows_limit", rapidjson::Value(wg.big_query_scan_rows_limit), allocator);
        root.PushBack(wg_item, allocator);
    }
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}

} // namespace starrocks
