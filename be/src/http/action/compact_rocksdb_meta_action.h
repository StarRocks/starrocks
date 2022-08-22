// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <map>
#include <mutex>

#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "http/http_handler.h"

namespace starrocks {

class ExecEnv;

class CompactRocksDbMetaAction : public HttpHandler {
public:
    explicit CompactRocksDbMetaAction(ExecEnv* exec_env) : _exec_env(exec_env) {}

    ~CompactRocksDbMetaAction() override = default;

    void handle(HttpRequest* req) override;

private:
    void _compact(HttpRequest* req);

    ExecEnv* _exec_env;
};

} // end namespace starrocks
