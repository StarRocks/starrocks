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

#include "runtime/stream_load/transaction_mgr.h"

#include <deque>
#include <future>
#include <sstream>

// use string iequal
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/http.h>
#include <fmt/format.h>
#include <rapidjson/prettywriter.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/logging.h"
#include "common/utils.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "http/http_channel.h"
#include "http/http_common.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/utils.h"
#include "runtime/client_cache.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "runtime/stream_load/transaction_mgr.h"
#include "util/byte_buffer.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
#include "util/json_util.h"
#include "util/metrics.h"
#include "util/starrocks_metrics.h"
#include "util/string_parser.hpp"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace starrocks {

METRIC_DEFINE_INT_COUNTER(transaction_streaming_load_requests_total, MetricUnit::REQUESTS);
METRIC_DEFINE_INT_COUNTER(transaction_streaming_load_bytes, MetricUnit::BYTES);
METRIC_DEFINE_INT_COUNTER(transaction_streaming_load_duration_ms, MetricUnit::MILLISECONDS);
METRIC_DEFINE_INT_GAUGE(transaction_streaming_load_current_processing, MetricUnit::REQUESTS);

#ifndef BE_TEST
static uint32_t interval = 30;
#else
static uint32_t interval = 1;
#endif

TransactionMgr::TransactionMgr(ExecEnv* exec_env) : _exec_env(exec_env) {
    StarRocksMetrics::instance()->metrics()->register_metric("transaction_streaming_load_requests_total",
                                                             &transaction_streaming_load_requests_total);
    StarRocksMetrics::instance()->metrics()->register_metric("transaction_streaming_load_bytes",
                                                             &transaction_streaming_load_bytes);
    StarRocksMetrics::instance()->metrics()->register_metric("transaction_streaming_load_duration_ms",
                                                             &transaction_streaming_load_duration_ms);
    StarRocksMetrics::instance()->metrics()->register_metric("transaction_streaming_load_current_processing",
                                                             &transaction_streaming_load_current_processing);
    _transaction_clean_thread = std::thread([this] {
#ifdef GOOGLE_PROFILER
        ProfilerRegisterThread();
#endif

        while (!_is_stopped.load()) {
            _clean_stream_context();
            sleep(interval);
        }
    });
    Thread::set_thread_name(_transaction_clean_thread, "transaction_clean");
}

TransactionMgr::~TransactionMgr() {
    _is_stopped.store(true);
    _transaction_clean_thread.join();
}

std::string TransactionMgr::_build_reply(const std::string& txn_op, StreamLoadContext* ctx) {
    return ctx->to_resp_json(txn_op, ctx->status);
}

std::string TransactionMgr::_build_reply(const std::string& label, const std::string& txn_op, const Status& st) {
    auto ctx = std::make_unique<StreamLoadContext>(_exec_env);
    ctx->label = label;
    return ctx->to_resp_json(txn_op, st);
}

Status TransactionMgr::list_transactions(const HttpRequest* req, std::string* resp) {
    auto ids = _exec_env->stream_context_mgr()->get_ids();

    rapidjson::StringBuffer s;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);
    for (const auto& id : ids) {
        std::string txn_resp;
        auto ctx = _exec_env->stream_context_mgr()->get(id);
        if (ctx != nullptr) {
            writer.StartObject();
            // try lock fail means transaction in processing
            if (ctx->lock.try_lock()) {
                writer.Key("Status");
                writer.String(to_string(ctx->status.code()).c_str());
                ctx->lock.unlock();
                writer.Key("InProcessing");
                writer.Bool(false);
            } else {
                writer.Key("InProcessing");
                writer.Bool(true);
            }
            writer.Key("Label");
            writer.String(ctx->label.c_str());
            writer.Key("TxnId");
            writer.Int64(ctx->txn_id);
            writer.Key("NumberTotalRows");
            writer.Int64(ctx->number_total_rows);
            writer.Key("NumberLoadedRows");
            writer.Int64(ctx->number_loaded_rows);
            writer.Key("NumberFilteredRows");
            writer.Int64(ctx->number_filtered_rows);
            writer.Key("NumberUnselectedRows");
            writer.Int64(ctx->number_unselected_rows);
            writer.Key("LoadBytes");
            writer.Int64(ctx->receive_bytes);
            writer.Key("StreamLoadPlanTimeMs");
            writer.Int64(ctx->stream_load_put_cost_nanos / 1000000);
            writer.Key("ReceivedDataTimeMs");
            writer.Int64(ctx->total_received_data_cost_nanos / 1000000);
            writer.Key("WriteDataTimeMs");
            writer.Int(ctx->write_data_cost_nanos / 1000000);
            writer.Key("BeginTimestamp");
            writer.Int64(ctx->begin_txn_ts);
            writer.Key("LastActiveTimestamp");
            writer.Int64(ctx->last_active_ts);
            writer.Key("TransactionTimeoutSec");
            writer.Int64(ctx->timeout_second);
            writer.Key("IdleTransactionTimeoutSec");
            writer.Int64(ctx->idle_timeout_sec);

            writer.EndObject();

            if (ctx->unref()) {
                delete ctx;
            }
        }
    }
    *resp = s.GetString();

    return Status::OK();
}

Status TransactionMgr::begin_transaction(const HttpRequest* req, std::string* resp) {
    auto label = req->header(HTTP_LABEL_KEY);
    Status st;
    auto ctx = _exec_env->stream_context_mgr()->get(label);
    if (ctx == nullptr) {
        ctx = new StreamLoadContext(_exec_env);
        ctx->ref();
        std::lock_guard<std::mutex> l(ctx->lock);
        st = _begin_transaction(req, ctx);
        if (!st.ok()) {
            ctx->status = st;
            if (ctx->need_rollback) {
                _rollback_transaction(ctx);
            }
        }
        LOG(INFO) << "new transaction manage request. " << ctx->brief() << ", tbl=" << ctx->table << " op=begin";
        *resp = _build_reply(TXN_BEGIN, ctx);
        if (ctx->unref()) {
            delete ctx;
        }
        return Status::OK();
    } else {
        LOG(INFO) << "new transaction req." << ctx->brief() << ", txn_op=" << TXN_BEGIN;
        if (ctx->unref()) {
            delete ctx;
        }
        *resp = _build_reply(label, TXN_BEGIN, Status::LabelAlreadyExists(""));
        return Status::LabelAlreadyExists("");
    }
}

Status TransactionMgr::rollback_transaction(const HttpRequest* req, std::string* resp) {
    auto label = req->header(HTTP_LABEL_KEY);
    Status st;
    auto ctx = _exec_env->stream_context_mgr()->get(label);
    if (ctx == nullptr) {
        st = Status::TransactionNotExists(fmt::format("transaction with label {} not exists", label));
    } else {
        LOG(INFO) << "new transaction manage request. " << ctx->brief() << ", tbl=" << ctx->table << " op=rollback";
        DeferOp defer([&] {
            if (ctx->unref()) {
                delete ctx;
            }
        });
        if (ctx->db != req->header(HTTP_DB_KEY)) {
            st = Status::InvalidArgument(
                    fmt::format("request db {} transaction db {} not match", req->header(HTTP_DB_KEY), ctx->db));
            *resp = _build_reply(label, TXN_ROLLBACK, st);
            return st;
        }
        ctx->status = Status::Aborted(fmt::format("transaction is aborted by user."));
        st = _rollback_transaction(ctx);
    }
    *resp = _build_reply(label, TXN_ROLLBACK, st);
    return st;
}

Status TransactionMgr::commit_transaction(const HttpRequest* req, std::string* resp) {
    auto label = req->header(HTTP_LABEL_KEY);
    Status st;
    auto ctx = _exec_env->stream_context_mgr()->get(label);
    if (ctx == nullptr) {
        st = Status::TransactionNotExists(fmt::format("transaction with label {} not exists", label));
        *resp = _build_reply(label, TXN_COMMIT, st);
        return st;
    } else {
        LOG(INFO) << "new transaction manage request. " << ctx->brief() << ", tbl=" << ctx->table
                  << " op=" << req->param(HTTP_TXN_OP_KEY);
        DeferOp defer([&] {
            if (ctx->unref()) {
                delete ctx;
            }
        });
        if (ctx->db != req->header(HTTP_DB_KEY)) {
            st = Status::InvalidArgument(
                    fmt::format("request db {} transaction db {} not match", req->header(HTTP_DB_KEY), ctx->db));
            *resp = _build_reply(label, TXN_COMMIT, st);
            return st;
        }
        if (!ctx->lock.try_lock()) {
            st = Status::TransactionInProcessing("Transaction in processing, please retry later");
            *resp = _build_reply(label, TXN_COMMIT, st);
            return st;
        }

        st = _commit_transaction(ctx, boost::iequals(TXN_PREPARE, req->param(HTTP_TXN_OP_KEY)));
        if (!st.ok()) {
            ctx->status = st;
            if (ctx->need_rollback) {
                _rollback_transaction(ctx);
            }
        }
        *resp = _build_reply(TXN_COMMIT, ctx);
        ctx->lock.unlock();
        return st;
    }
}

Status TransactionMgr::_begin_transaction(const HttpRequest* req, StreamLoadContext* ctx) {
    // 1. parse request
    ctx->load_type = TLoadType::MANUAL_LOAD;
    ctx->load_src_type = TLoadSourceType::RAW;

    ctx->db = req->header(HTTP_DB_KEY);
    ctx->table = req->header(HTTP_TABLE_KEY);
    ctx->label = req->header(HTTP_LABEL_KEY);

    if (!req->header(HTTP_TIMEOUT).empty()) {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        const auto& timeout = req->header(HTTP_TIMEOUT);
        auto timeout_second =
                StringParser::string_to_unsigned_int<int32_t>(timeout.c_str(), timeout.length(), &parse_result);
        if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
            return Status::InvalidArgument("Invalid timeout format");
        }
        ctx->timeout_second = timeout_second;
    }
    if (!req->header(HTTP_IDLE_TRANSACTION_TIMEOUT).empty()) {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        const auto& timeout = req->header(HTTP_IDLE_TRANSACTION_TIMEOUT);
        auto timeout_second =
                StringParser::string_to_unsigned_int<int32_t>(timeout.c_str(), timeout.length(), &parse_result);
        if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
            return Status::InvalidArgument("Invalid timeout format");
        }
        ctx->idle_timeout_sec = timeout_second;
    }

    if (!parse_basic_auth(*req, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed." << ctx->brief();
        return Status::InternalError("no valid Basic authorization");
    }

    // 2. begin transaction
    ctx->begin_txn_ts = UnixSeconds();
    int64_t begin_nanos = MonotonicNanos();
    ctx->last_active_ts = ctx->begin_txn_ts;
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->begin_txn(ctx));
    ctx->begin_txn_cost_nanos = MonotonicNanos() - begin_nanos;

    // 4. put load stream context
    RETURN_IF_ERROR(_exec_env->stream_context_mgr()->put(ctx->label, ctx));

    transaction_streaming_load_current_processing.increment(1);
    transaction_streaming_load_requests_total.increment(1);

    return Status::OK();
}

Status TransactionMgr::_commit_transaction(StreamLoadContext* ctx, bool prepare) {
    if (!ctx->status.ok()) {
        return Status::Aborted(fmt::format("transaction is aborted"));
    }

    if (ctx->body_sink != nullptr) {
        // 1. finish stream pipe & wait it done
        if (ctx->buffer != nullptr && ctx->buffer->pos > 0) {
            ctx->buffer->flip();
            ctx->body_sink->append(std::move(ctx->buffer));
            ctx->buffer = nullptr;
        }
        RETURN_IF_ERROR(ctx->body_sink->finish());
        RETURN_IF_ERROR(ctx->future.get());
    } else {
        return Status::InternalError(fmt::format("transaction {} hasn't send any data yet", ctx->brief()));
    }

    // 2. remove stream pipe
    _exec_env->load_stream_mgr()->remove(ctx->id);

    // 3. commit transaction
    int64_t commit_and_publish_start_time = MonotonicNanos();
    if (prepare) {
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->prepare_txn(ctx));
    } else {
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->commit_txn(ctx));
    }
    ctx->commit_and_publish_txn_cost_nanos = MonotonicNanos() - commit_and_publish_start_time;

    // 4. remove stream load context
    _exec_env->stream_context_mgr()->remove(ctx->label);

    ctx->last_active_ts = UnixSeconds();
    ctx->load_cost_nanos = MonotonicNanos() - ctx->start_nanos;
    transaction_streaming_load_duration_ms.increment(ctx->load_cost_nanos / 1000000);
    transaction_streaming_load_bytes.increment(ctx->receive_bytes);
    transaction_streaming_load_current_processing.increment(-1);

    return Status::OK();
}

// this function must be thread-safe so that we can call anytime
Status TransactionMgr::_rollback_transaction(StreamLoadContext* ctx) {
    LOG(INFO) << "Rollback transaction " << ctx->brief();
    // 1. cancel stream pipe
    if (ctx->body_sink != nullptr) {
        if (ctx->status.ok()) {
            ctx->status = Status::Aborted(fmt::format("transaction is aborted by system."));
        }
        ctx->body_sink->cancel(ctx->status);
    }

    // 2. remove stream pipe
    _exec_env->load_stream_mgr()->remove(ctx->id);

    // 3. rollback transaction by send request to FE
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->rollback_txn(ctx));

    // 4. remove stream load context
    //    By remove context at the end, we can retry when the rollback FE fails
    _exec_env->stream_context_mgr()->remove(ctx->label);

    transaction_streaming_load_current_processing.increment(-1);

    return Status::OK();
}

void TransactionMgr::_clean_stream_context() {
    auto ids = _exec_env->stream_context_mgr()->get_ids();

    for (const auto& id : ids) {
        auto ctx = _exec_env->stream_context_mgr()->get(id);
        if (ctx != nullptr) {
            int64_t now = UnixSeconds();
            // try lock fail means transaction in processing
            if (ctx->lock.try_lock()) {
                // abort timeout transaction
                if ((now - ctx->begin_txn_ts) > ctx->timeout_second && ctx->timeout_second > 0) {
                    ctx->status = Status::Aborted(fmt::format("transaction is aborted by timeout."));
                    auto st = _rollback_transaction(ctx);
                    LOG(INFO) << "Abort transaction " << ctx->brief() << " since timeout " << ctx->timeout_second
                              << " begin ts " << ctx->begin_txn_ts << " status " << st;
                }

                if (ctx->body_sink != nullptr) {
                    if (!ctx->body_sink->exhausted()) {
                        ctx->last_active_ts = UnixSeconds();
                    }
                }

                if ((now - ctx->last_active_ts) > ctx->idle_timeout_sec + interval && ctx->idle_timeout_sec > 0) {
                    ctx->status = Status::Aborted(fmt::format("transaction is aborted by idle timeout."));
                    auto st = _rollback_transaction(ctx);
                    LOG(INFO) << "Abort transaction " << ctx->brief() << " since idle timeout "
                              << ctx->idle_timeout_sec + interval << " last active ts " << ctx->last_active_ts
                              << " status " << st;
                }
                ctx->lock.unlock();
            }
            if (ctx->unref()) {
                delete ctx;
            }
        }
    }
}

} // namespace starrocks
