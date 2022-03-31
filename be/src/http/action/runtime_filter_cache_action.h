// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#pragma once

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
    ExecEnv* _exec_env;
    std::once_flag _once_flag;
    std::unordered_map<std::string, std::function<void()>> _config_callback;
};

} // namespace starrocks
