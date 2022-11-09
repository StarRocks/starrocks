// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

class QueryCacheAction : public HttpHandler {
public:
    explicit QueryCacheAction(ExecEnv* exec_env) : _exec_env(exec_env) {}
    ~QueryCacheAction() override = default;

    void handle(HttpRequest* req) override;

private:
    void _handle(HttpRequest* req, const std::function<void(rapidjson::Document& root)>& func);
    void _handle_stat(HttpRequest* req);
    void _handle_invalidate_all(HttpRequest* req);
    void _handle_error(HttpRequest* req, const std::string& error_msg);
    ExecEnv* _exec_env;
};

} // namespace starrocks
