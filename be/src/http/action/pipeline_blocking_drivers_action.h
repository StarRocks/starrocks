// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <functional>
#include <mutex>
#include <unordered_map>

#include "http/http_handler.h"
#include "runtime/exec_env.h"

namespace starrocks {

class PipelineBlockingDriversAction : public HttpHandler {
public:
    explicit PipelineBlockingDriversAction(ExecEnv* exec_env) : _exec_env(exec_env) {}
    ~PipelineBlockingDriversAction() override = default;

    void handle(HttpRequest* req) override;

private:
    void _handle(HttpRequest* req, std::function<void(rapidjson::Document& root)> func);
    // Returns information about the blocking drivers with the following format:
    // {
    //      "queries_not_in_workgroup": [{
    //          "query_id": "str",
    //          "fragments": [{
    //              "fragment_id": "str",
    //              "drivers": [{
    //                  "driver_id": "int",
    //                  "state": "str",
    //                  "driver_desc": "str"
    //              }]
    //          }]
    //      }],
    //      "queries_in_workgroup": []
    // }
    void _handle_stat(HttpRequest* req);
    void _handle_error(HttpRequest* req, const std::string& error_msg);

private:
    ExecEnv* _exec_env;
};

} // namespace starrocks
