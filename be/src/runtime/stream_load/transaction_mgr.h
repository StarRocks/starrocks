// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

#include "stream_load_context.h"
#include "util/starrocks_metrics.h"
#include "util/thread.h"
#include "util/uid_util.h" // for std::hash for UniqueId

namespace starrocks {

class HttpRequest;

// used to register all streams in process so that other module can get this stream
class StreamContextMgr {
public:
    StreamContextMgr() {}

    ~StreamContextMgr() {
        std::lock_guard<std::mutex> l(_lock);
        for (auto& iter : _stream_map) {
            if (iter.second->unref()) {
                delete iter.second;
            }
        }
    }

    Status put(const std::string& id, StreamLoadContext* stream) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it != std::end(_stream_map)) {
            return Status::InternalError("id already exist");
        }
        _stream_map.emplace(id, stream);
        stream->ref();
        VLOG(3) << "put stream load pipe: " << id;
        return Status::OK();
    }

    StreamLoadContext* get(const std::string& id) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it == std::end(_stream_map)) {
            return nullptr;
        }
        auto stream = it->second;
        stream->ref();
        return stream;
    }

    void remove(const std::string& id) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it != std::end(_stream_map)) {
            if (it->second->unref()) {
                delete it->second;
            }
            _stream_map.erase(it);
            VLOG(3) << "remove stream load pipe: " << id;
        }
    }

    std::vector<std::string> get_ids() {
        std::vector<std::string> ids;
        {
            std::lock_guard<std::mutex> l(_lock);
            for (auto& iter : _stream_map) {
                ids.emplace_back(iter.first);
            }
        }

        return ids;
    }

private:
    std::mutex _lock;
    std::unordered_map<std::string, StreamLoadContext*> _stream_map;
};

class TransactionMgr {
public:
    explicit TransactionMgr(ExecEnv* exec_env);
    ~TransactionMgr();

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
    std::thread _transaction_clean_thread;
    std::atomic<bool> _is_stopped = false;
};

} // namespace starrocks
