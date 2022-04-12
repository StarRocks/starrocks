// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

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

// Update BE config.
class RuntimeFilterCacheAction : public HttpHandler {
public:
    explicit RuntimeFilterCacheAction(ExecEnv* exec_env) : _exec_env(exec_env) {}
    ~RuntimeFilterCacheAction() override = default;

    void handle(HttpRequest* req) override;

private:
    void _handle(HttpRequest* req, std::function<void(rapidjson::Document& root)> func);
    void _handle_stat(HttpRequest* req);
    void _handle_trace(HttpRequest* req);
    void _handle_trace_switch(HttpRequest* req, bool on);
    void _handle_error(HttpRequest* req, const std::string& error_msg);
    ExecEnv* _exec_env;
};

} // namespace starrocks
