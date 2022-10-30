// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <functional>

#include "gen_cpp/PlanNodes_types.h"
#include "http/http_handler.h"
#include "runtime/client_cache.h"
#include "runtime/mem_tracker.h"
#include "runtime/message_body_sink.h"

namespace starrocks {

class ExecEnv;
class Status;
class StreamLoadContext;
class TStreamLoadPutRequest;

class TransactionManagerAction : public HttpHandler {
public:
    explicit TransactionManagerAction(ExecEnv* exec_env);
    ~TransactionManagerAction() override;

    void handle(HttpRequest* req) override;

private:
    void _send_error_reply(HttpRequest* req, const Status& st);

    ExecEnv* _exec_env;
};

class TransactionStreamLoadAction : public HttpHandler {
public:
    explicit TransactionStreamLoadAction(ExecEnv* exec_env);
    ~TransactionStreamLoadAction() override;

    void handle(HttpRequest* req) override;

    bool request_will_be_read_progressively() override { return true; }

    int on_header(HttpRequest* req) override;

    void on_chunk_data(HttpRequest* req) override;

private:
    Status _on_header(HttpRequest* http_req, StreamLoadContext* ctx);
    Status _exec_plan_fragment(HttpRequest* http_req, StreamLoadContext* ctx);
    void _send_error_reply(HttpRequest* req, const Status& st);
    Status _parse_request(HttpRequest* http_req, StreamLoadContext* ctx, TStreamLoadPutRequest& request);

    ExecEnv* _exec_env;
};

} // namespace starrocks
