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

#pragma once

#include <atomic>
#include <string>
#include <thread>

#include "common/status.h"

namespace starrocks {

class ExecEnv;
class HttpRequest;
class StreamLoadContext;
class StreamLoadExecutor;

class TransactionMgr {
public:
    TransactionMgr(ExecEnv* exec_env, StreamLoadExecutor* stream_load_executor);
    ~TransactionMgr();

    void stop();

    Status begin_transaction(const HttpRequest* req, std::string* resp);
    Status commit_transaction(const HttpRequest* req, std::string* resp);
    Status rollback_transaction(const HttpRequest* req, std::string* resp);
    Status list_transactions(const HttpRequest* req, std::string* resp);

    Status _begin_transaction(const HttpRequest* http_req, StreamLoadContext* ctx);
    Status _commit_transaction(StreamLoadContext* ctx, bool prepare);
    Status _rollback_transaction(StreamLoadContext* ctx);

    std::string _build_reply(const std::string& txn_op, StreamLoadContext* ctx);
    std::string _build_reply(const std::string& label, const std::string& txn_op, const Status& st);

private:
    void _clean_stream_context();

    ExecEnv* _exec_env;
    StreamLoadExecutor* _stream_load_executor;
    std::thread _transaction_clean_thread;
    std::atomic<bool> _is_stopped = false;
};

} // namespace starrocks
