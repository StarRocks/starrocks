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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/action/stream_load.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/action/stream_load.h"

#include <deque>
#include <future>
#include <sstream>

// use string iequal
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/http.h>
#include <rapidjson/prettywriter.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "agent/master_info.h"
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
#include "simdjson.h"
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

METRIC_DEFINE_INT_COUNTER(streaming_load_requests_total, MetricUnit::REQUESTS);
METRIC_DEFINE_INT_COUNTER(streaming_load_bytes, MetricUnit::BYTES);
METRIC_DEFINE_INT_COUNTER(streaming_load_duration_ms, MetricUnit::MILLISECONDS);
METRIC_DEFINE_INT_GAUGE(streaming_load_current_processing, MetricUnit::REQUESTS);

#ifdef BE_TEST
TStreamLoadPutResult k_stream_load_put_result;
#endif

static TFileFormatType::type parse_format(const std::string& format_str) {
    if (boost::iequals(format_str, "csv")) {
        return TFileFormatType::FORMAT_CSV_PLAIN;
    } else if (boost::iequals(format_str, "json")) {
        return TFileFormatType::FORMAT_JSON;
    } else if (boost::iequals(format_str, "gzip")) {
        return TFileFormatType::FORMAT_CSV_GZ;
    } else if (boost::iequals(format_str, "bzip2")) {
        return TFileFormatType::FORMAT_CSV_BZ2;
    } else if (boost::iequals(format_str, "lz4")) {
        return TFileFormatType::FORMAT_CSV_LZ4_FRAME;
    } else if (boost::iequals(format_str, "deflate")) {
        return TFileFormatType::FORMAT_CSV_DEFLATE;
    } else if (boost::iequals(format_str, "zstd")) {
        return TFileFormatType::FORMAT_CSV_ZSTD;
    }
    return TFileFormatType::FORMAT_UNKNOWN;
}

static bool is_format_support_streaming(TFileFormatType::type format) {
    switch (format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_LZ4_FRAME:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
    case TFileFormatType::FORMAT_CSV_ZSTD:
    case TFileFormatType::FORMAT_JSON:
        return true;
    default:
        return false;
    }
}

StreamLoadAction::StreamLoadAction(ExecEnv* exec_env, ConcurrentLimiter* limiter)
        : _exec_env(exec_env), _http_concurrent_limiter(limiter) {
    StarRocksMetrics::instance()->metrics()->register_metric("streaming_load_requests_total",
                                                             &streaming_load_requests_total);
    StarRocksMetrics::instance()->metrics()->register_metric("streaming_load_bytes", &streaming_load_bytes);
    StarRocksMetrics::instance()->metrics()->register_metric("streaming_load_duration_ms", &streaming_load_duration_ms);
    StarRocksMetrics::instance()->metrics()->register_metric("streaming_load_current_processing",
                                                             &streaming_load_current_processing);
}

StreamLoadAction::~StreamLoadAction() = default;

void StreamLoadAction::handle(HttpRequest* req) {
    auto* ctx = (StreamLoadContext*)req->handler_ctx();
    if (ctx == nullptr) {
        return;
    }

    // status already set to fail
    if (ctx->status.ok()) {
        ctx->status = _handle(ctx);
        if (!ctx->status.ok() && ctx->status.code() != TStatusCode::PUBLISH_TIMEOUT) {
            LOG(WARNING) << "Fail to handle streaming load, id=" << ctx->id
                         << " errmsg=" << ctx->status.get_error_msg();
        }
    }
    ctx->load_cost_nanos = MonotonicNanos() - ctx->start_nanos;

    if (!ctx->status.ok() && ctx->status.code() != TStatusCode::PUBLISH_TIMEOUT) {
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx);
            ctx->need_rollback = false;
        }
        if (ctx->body_sink != nullptr) {
            ctx->body_sink->cancel(ctx->status);
        }
    }

    auto str = ctx->to_json();
    HttpChannel::send_reply(req, str);

    // update statstics
    streaming_load_requests_total.increment(1);
    streaming_load_duration_ms.increment(ctx->load_cost_nanos / 1000000);
    streaming_load_bytes.increment(ctx->receive_bytes);
    streaming_load_current_processing.increment(-1);
}

Status StreamLoadAction::_handle(StreamLoadContext* ctx) {
    if (ctx->body_bytes > 0 && ctx->receive_bytes != ctx->body_bytes) {
        LOG(WARNING) << "receive body don't equal with body bytes, body_bytes=" << ctx->body_bytes
                     << ", receive_bytes=" << ctx->receive_bytes << ", id=" << ctx->id;
        return Status::InternalError("receive body don't equal with body bytes");
    }
    if (!ctx->use_streaming) {
        // if we use non-streaming, we need to close file first,
        // then execute_plan_fragment here
        // this will close file
        ctx->body_sink.reset();
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(ctx));
    } else {
        if (ctx->buffer != nullptr && ctx->buffer->pos > 0) {
            ctx->buffer->flip();
            ctx->body_sink->append(std::move(ctx->buffer));
            ctx->buffer = nullptr;
        }
        RETURN_IF_ERROR(ctx->body_sink->finish());
    }

    // wait stream load finish
    RETURN_IF_ERROR(ctx->future.get());

    // If put file succeess we need commit this load
    int64_t commit_and_publish_start_time = MonotonicNanos();
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->commit_txn(ctx));
    ctx->commit_and_publish_txn_cost_nanos = MonotonicNanos() - commit_and_publish_start_time;

    return Status::OK();
}

int StreamLoadAction::on_header(HttpRequest* req) {
    streaming_load_current_processing.increment(1);

    auto* ctx = new StreamLoadContext(_exec_env);
    ctx->ref();
    req->set_handler_ctx(ctx);

    ctx->load_type = TLoadType::MANUAL_LOAD;
    ctx->load_src_type = TLoadSourceType::RAW;

    ctx->db = req->param(HTTP_DB_KEY);
    ctx->table = req->param(HTTP_TABLE_KEY);
    ctx->label = req->header(HTTP_LABEL_KEY);
    if (ctx->label.empty()) {
        ctx->label = generate_uuid_string();
    }

    if (config::enable_http_stream_load_limit && !ctx->check_and_set_http_limiter(_http_concurrent_limiter)) {
        LOG(WARNING) << "income streaming load request hit limit." << ctx->brief() << ", db=" << ctx->db
                     << ", tbl=" << ctx->table;
        ctx->status =
                Status::ResourceBusy(fmt::format("Stream Load exceed http cuncurrent limit {}, please try again later",
                                                 config::be_http_num_workers - 1));
        auto str = ctx->to_json();
        HttpChannel::send_reply(req, str);
        return -1;
    } else {
        LOG(INFO) << "new income streaming load request." << ctx->brief() << ", db=" << ctx->db
                  << ", tbl=" << ctx->table;
    }

    VLOG(1) << "streaming load request: " << req->debug_string();

    auto st = _on_header(req, ctx);
    if (!st.ok()) {
        ctx->status = st;
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx);
            ctx->need_rollback = false;
        }
        if (ctx->body_sink != nullptr) {
            ctx->body_sink->cancel(st);
        }
        auto str = ctx->to_json();
        HttpChannel::send_reply(req, str);
        streaming_load_current_processing.increment(-1);
        return -1;
    }
    return 0;
}

Status StreamLoadAction::_on_header(HttpRequest* http_req, StreamLoadContext* ctx) {
    // auth information
    if (!parse_basic_auth(*http_req, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed." << ctx->brief();
        return Status::InternalError("no valid Basic authorization");
    }
    // check content length
    ctx->body_bytes = 0;
    size_t max_body_bytes = config::streaming_load_max_mb * 1024 * 1024;
    if (!http_req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
        ctx->body_bytes = std::stol(http_req->header(HttpHeaders::CONTENT_LENGTH));
        if (ctx->body_bytes > max_body_bytes) {
            std::stringstream ss;
            ss << "body size " << ctx->body_bytes << " exceed limit: " << max_body_bytes << ", " << ctx->brief()
               << ". You can increase the limit by setting streaming_load_max_mb in be.conf.";
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        if (ctx->format == TFileFormatType::FORMAT_JSON) {
            // Allocate buffer in advance, since the json payload cannot be parsed in stream mode.
            // For efficiency reasons, simdjson requires a string with a few bytes (simdjson::SIMDJSON_PADDING) at the end.
            ctx->buffer = ByteBuffer::allocate(ctx->body_bytes + simdjson::SIMDJSON_PADDING);
        }
    } else {
#ifndef BE_TEST
        evhttp_connection_set_max_body_size(evhttp_request_get_connection(http_req->get_evhttp_request()),
                                            max_body_bytes);
#endif
    }
    // get format of this put
    if (http_req->header(HTTP_FORMAT_KEY).empty()) {
        ctx->format = TFileFormatType::FORMAT_CSV_PLAIN;
    } else {
        ctx->format = parse_format(http_req->header(HTTP_FORMAT_KEY));
        if (ctx->format == TFileFormatType::FORMAT_UNKNOWN) {
            std::stringstream ss;
            ss << "unknown data format, format=" << http_req->header(HTTP_FORMAT_KEY);
            return Status::InternalError(ss.str());
        }

        if (ctx->format == TFileFormatType::FORMAT_JSON) {
            size_t max_body_bytes = config::streaming_load_max_batch_size_mb * 1024 * 1024;
            auto ignore_json_size = boost::iequals(http_req->header(HTTP_IGNORE_JSON_SIZE), "true");
            if (!ignore_json_size && ctx->body_bytes > max_body_bytes) {
                std::stringstream ss;
                ss << "The size of this batch exceed the max size [" << max_body_bytes << "]  of json type data "
                   << " data [ " << ctx->body_bytes
                   << " ]. Set ignore_json_size to skip the check, although it may lead huge memory consuming.";
                return Status::InternalError(ss.str());
            }
        }
    }

    if (!http_req->header(HTTP_TIMEOUT).empty()) {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        const auto& timeout = http_req->header(HTTP_TIMEOUT);
        auto timeout_second =
                StringParser::string_to_unsigned_int<int32_t>(timeout.c_str(), timeout.length(), &parse_result);
        if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
            return Status::InvalidArgument("Invalid timeout format");
        }
        ctx->timeout_second = timeout_second;
    }

    // begin transaction
    int64_t begin_txn_start_time = MonotonicNanos();
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->begin_txn(ctx));
    ctx->begin_txn_cost_nanos = MonotonicNanos() - begin_txn_start_time;

    // process put file
    return _process_put(http_req, ctx);
}

void StreamLoadAction::on_chunk_data(HttpRequest* req) {
    auto* ctx = (StreamLoadContext*)req->handler_ctx();
    if (ctx == nullptr || !ctx->status.ok()) {
        return;
    }

    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(ctx->instance_mem_tracker.get());

    struct evhttp_request* ev_req = req->get_evhttp_request();
    auto evbuf = evhttp_request_get_input_buffer(ev_req);

    int64_t start_read_data_time = MonotonicNanos();

    size_t len = 0;
    while ((len = evbuffer_get_length(evbuf)) > 0) {
        if (ctx->buffer == nullptr) {
            // Initialize buffer.
            ctx->buffer = ByteBuffer::allocate(
                    ctx->format == TFileFormatType::FORMAT_JSON ? std::max(len, ctx->kDefaultBufferSize) : len);

        } else if (ctx->buffer->remaining() < len) {
            if (ctx->format == TFileFormatType::FORMAT_JSON) {
                // For json format, we need build a complete json before we push the buffer to the pipe.
                // buffer capacity is not enough, so we try to expand the buffer.
                ByteBufferPtr buf = ByteBuffer::allocate(BitUtil::RoundUpToPowerOfTwo(ctx->buffer->pos + len));
                buf->put_bytes(ctx->buffer->ptr, ctx->buffer->pos);
                std::swap(buf, ctx->buffer);

            } else {
                // For non-json format, we could push buffer to the body_sink in streaming mode.
                // buffer capacity is not enough, so we push the buffer to the pipe and allocate new one.
                ctx->buffer->flip();
                auto st = ctx->body_sink->append(std::move(ctx->buffer));
                if (!st.ok()) {
                    LOG(WARNING) << "append body content failed. errmsg=" << st << " context=" << ctx->brief();
                    ctx->status = st;
                    return;
                }

                ctx->buffer = ByteBuffer::allocate(std::max(len, ctx->kDefaultBufferSize));
            }
        }

        int remove_bytes;
        {
            // The memory is applied for in http server thread,
            // so the release of this memory must be recorded in ProcessMemTracker
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
            remove_bytes = evbuffer_remove(evbuf, ctx->buffer->ptr + ctx->buffer->pos, ctx->buffer->remaining());
        }
        ctx->buffer->pos += remove_bytes;
        ctx->receive_bytes += remove_bytes;
    }
    ctx->total_received_data_cost_nanos += (MonotonicNanos() - start_read_data_time);
}

void StreamLoadAction::free_handler_ctx(void* param) {
    auto* ctx = (StreamLoadContext*)param;
    if (ctx == nullptr) {
        return;
    }
    // sender is going, make receiver know it
    if (ctx->body_sink != nullptr) {
        ctx->body_sink->cancel(Status::Cancelled("Cancelled"));
    }
    _exec_env->load_stream_mgr()->remove(ctx->id);
    if (ctx->unref()) {
        delete ctx;
    }
}

Status StreamLoadAction::_process_put(HttpRequest* http_req, StreamLoadContext* ctx) {
    // Now we use stream
    ctx->use_streaming = is_format_support_streaming(ctx->format);

    // put request
    TStreamLoadPutRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.formatType = ctx->format;
    request.__set_loadId(ctx->id.to_thrift());
    if (ctx->use_streaming) {
        auto pipe =
                std::make_shared<StreamLoadPipe>(1024 * 1024 /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */);
        RETURN_IF_ERROR(_exec_env->load_stream_mgr()->put(ctx->id, pipe));
        request.fileType = TFileType::FILE_STREAM;
        ctx->body_sink = pipe;
    } else {
        RETURN_IF_ERROR(_data_saved_path(http_req, &request.path));
        auto file_sink = std::make_shared<MessageBodyFileSink>(request.path);
        RETURN_IF_ERROR(file_sink->open());
        request.__isset.path = true;
        request.fileType = TFileType::FILE_LOCAL;
        ctx->body_sink = file_sink;
    }
    if (!http_req->header(HTTP_COLUMNS).empty()) {
        request.__set_columns(http_req->header(HTTP_COLUMNS));
    }
    if (!http_req->header(HTTP_WHERE).empty()) {
        request.__set_where(http_req->header(HTTP_WHERE));
    }
    if (!http_req->header(HTTP_COLUMN_SEPARATOR).empty()) {
        request.__set_columnSeparator(http_req->header(HTTP_COLUMN_SEPARATOR));
    }
    if (!http_req->header(HTTP_ROW_DELIMITER).empty()) {
        request.__set_rowDelimiter(http_req->header(HTTP_ROW_DELIMITER));
    }
    if (!http_req->header(HTTP_SKIP_HEADER).empty()) {
        try {
            auto skip_header = std::stoll(http_req->header(HTTP_SKIP_HEADER));
            if (skip_header < 0) {
                return Status::InvalidArgument("skip_header must be equal or greater than 0");
            }
            request.__set_skipHeader(skip_header);
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid csv load skip_header format");
        }
    }
    if (!http_req->header(HTTP_TRIM_SPACE).empty()) {
        if (boost::iequals(http_req->header(HTTP_TRIM_SPACE), "false")) {
            request.__set_trimSpace(false);
        } else if (boost::iequals(http_req->header(HTTP_TRIM_SPACE), "true")) {
            request.__set_trimSpace(true);
        } else {
            return Status::InvalidArgument("Invalid trim space format. Must be bool type");
        }
    }
    if (!http_req->header(HTTP_ENCLOSE).empty() && http_req->header(HTTP_ENCLOSE).size() > 0) {
        request.__set_enclose(http_req->header(HTTP_ENCLOSE)[0]);
    }
    if (!http_req->header(HTTP_ESCAPE).empty() && http_req->header(HTTP_ESCAPE).size() > 0) {
        request.__set_escape(http_req->header(HTTP_ESCAPE)[0]);
    }
    if (!http_req->header(HTTP_PARTITIONS).empty()) {
        request.__set_partitions(http_req->header(HTTP_PARTITIONS));
        request.__set_isTempPartition(false);
        if (!http_req->header(HTTP_TEMP_PARTITIONS).empty()) {
            return Status::InvalidArgument("Can not specify both partitions and temporary partitions");
        }
    }
    if (!http_req->header(HTTP_TEMP_PARTITIONS).empty()) {
        request.__set_partitions(http_req->header(HTTP_TEMP_PARTITIONS));
        request.__set_isTempPartition(true);
        if (!http_req->header(HTTP_PARTITIONS).empty()) {
            return Status::InvalidArgument("Can not specify both partitions and temporary partitions");
        }
    }
    if (!http_req->header(HTTP_NEGATIVE).empty() && http_req->header(HTTP_NEGATIVE) == "true") {
        request.__set_negative(true);
    } else {
        request.__set_negative(false);
    }
    if (!http_req->header(HTTP_STRICT_MODE).empty()) {
        if (boost::iequals(http_req->header(HTTP_STRICT_MODE), "false")) {
            request.__set_strictMode(false);
        } else if (boost::iequals(http_req->header(HTTP_STRICT_MODE), "true")) {
            request.__set_strictMode(true);
        } else {
            return Status::InvalidArgument("Invalid strict mode format. Must be bool type");
        }
    }
    if (!http_req->header(HTTP_TIMEZONE).empty()) {
        request.__set_timezone(http_req->header(HTTP_TIMEZONE));
    }
    if (!http_req->header(HTTP_LOAD_MEM_LIMIT).empty()) {
        try {
            auto load_mem_limit = std::stoll(http_req->header(HTTP_LOAD_MEM_LIMIT));
            if (load_mem_limit < 0) {
                return Status::InvalidArgument("load_mem_limit must be equal or greater than 0");
            }
            request.__set_loadMemLimit(load_mem_limit);
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid load mem limit format");
        }
    }
    if (!http_req->header(HTTP_JSONPATHS).empty()) {
        request.__set_jsonpaths(http_req->header(HTTP_JSONPATHS));
    }
    if (!http_req->header(HTTP_JSONROOT).empty()) {
        request.__set_json_root(http_req->header(HTTP_JSONROOT));
    }
    if (!http_req->header(HTTP_STRIP_OUTER_ARRAY).empty()) {
        if (boost::iequals(http_req->header(HTTP_STRIP_OUTER_ARRAY), "true")) {
            request.__set_strip_outer_array(true);
        } else {
            request.__set_strip_outer_array(false);
        }
    } else {
        request.__set_strip_outer_array(false);
    }
    if (!http_req->header(HTTP_PARTIAL_UPDATE).empty()) {
        if (boost::iequals(http_req->header(HTTP_PARTIAL_UPDATE), "false")) {
            request.__set_partial_update(false);
        } else if (boost::iequals(http_req->header(HTTP_PARTIAL_UPDATE), "true")) {
            request.__set_partial_update(true);
        } else {
            return Status::InvalidArgument("Invalid partial update flag format. Must be bool type");
        }
    }
    if (!http_req->header(HTTP_MERGE_CONDITION).empty()) {
        request.__set_merge_condition(http_req->header(HTTP_MERGE_CONDITION));
    }
    if (!http_req->header(HTTP_TRANSMISSION_COMPRESSION_TYPE).empty()) {
        request.__set_transmission_compression_type(http_req->header(HTTP_TRANSMISSION_COMPRESSION_TYPE));
    }
    if (!http_req->header(HTTP_LOAD_DOP).empty()) {
        try {
            auto parallel_request_num = std::stoll(http_req->header(HTTP_LOAD_DOP));
            request.__set_load_dop(parallel_request_num);
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid load_dop format");
        }
    }
    int32_t rpc_timeout_ms = config::txn_commit_rpc_timeout_ms;
    if (ctx->timeout_second != -1) {
        request.__set_timeout(ctx->timeout_second);
        rpc_timeout_ms = std::min(ctx->timeout_second * 1000, config::txn_commit_rpc_timeout_ms);
    }
    request.__set_thrift_rpc_timeout_ms(rpc_timeout_ms);
    // plan this load
    TNetworkAddress master_addr = get_master_address();
#ifndef BE_TEST
    if (!http_req->header(HTTP_MAX_FILTER_RATIO).empty()) {
        ctx->max_filter_ratio = strtod(http_req->header(HTTP_MAX_FILTER_RATIO).c_str(), nullptr);
    }

    int64_t stream_load_put_start_time = MonotonicNanos();
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, ctx](FrontendServiceConnection& client) { client->streamLoadPut(ctx->put_result, request); },
            rpc_timeout_ms));
    ctx->stream_load_put_cost_nanos = MonotonicNanos() - stream_load_put_start_time;
#else
    ctx->put_result = k_stream_load_put_result;
#endif
    Status plan_status(ctx->put_result.status);
    if (!plan_status.ok()) {
        LOG(WARNING) << "plan streaming load failed. errmsg=" << plan_status.get_error_msg() << ctx->brief();
        return plan_status;
    }
    VLOG(3) << "params is " << apache::thrift::ThriftDebugString(ctx->put_result.params);
    // if we not use streaming, we must download total content before we begin
    // to process this load
    if (!ctx->use_streaming) {
        return Status::OK();
    }

    if (!http_req->header(HTTP_EXEC_MEM_LIMIT).empty()) {
        auto exec_mem_limit = std::stoll(http_req->header(HTTP_EXEC_MEM_LIMIT));
        if (exec_mem_limit <= 0) {
            return Status::InvalidArgument("exec_mem_limit must be greater than 0");
        }
        ctx->put_result.params.query_options.mem_limit = exec_mem_limit;
    }

    return _exec_env->stream_load_executor()->execute_plan_fragment(ctx);
}

Status StreamLoadAction::_data_saved_path(HttpRequest* req, std::string* file_path) {
    std::string prefix;
    RETURN_IF_ERROR(_exec_env->load_path_mgr()->allocate_dir(req->param(HTTP_DB_KEY), "", &prefix));
    timeval tv;
    gettimeofday(&tv, nullptr);
    struct tm tm;
    time_t cur_sec = tv.tv_sec;
    localtime_r(&cur_sec, &tm);
    char buf[64];
    strftime(buf, 64, "%Y%m%d%H%M%S", &tm);
    std::stringstream ss;
    ss << prefix << "/" << req->param(HTTP_TABLE_KEY) << "." << buf << "." << tv.tv_usec;
    *file_path = ss.str();
    return Status::OK();
}

} // namespace starrocks
