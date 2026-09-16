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

#include "compute_env/load_path/load_path_state_helper.h"

#include <atomic>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>

#include "common/logging.h"
#include "compute_env/load_path/base_load_path_mgr.h"
#include "compute_env/load_path/rejected_record_writer.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks {

namespace {

constexpr int64_t kMaxErrorNum = 50;

BaseLoadPathMgr* load_path_mgr(RuntimeState* state) {
    if (state == nullptr || state->query_execution_services() == nullptr ||
        state->query_execution_services()->runtime == nullptr) {
        return nullptr;
    }
    return state->query_execution_services()->runtime->load_path_mgr;
}

} // namespace

Status LoadPathStateHelper::create_error_log_file(RuntimeState* state) {
    auto* mgr = load_path_mgr(state);
    if (mgr == nullptr) {
        return Status::InternalError("load_path_mgr is not initialized");
    }
    RETURN_IF_ERROR(mgr->get_load_error_file_name(state->_fragment_instance_id, &state->_error_log_file_path));
    std::string error_log_absolute_path = mgr->get_load_error_absolute_path(state->_error_log_file_path);
    state->_error_log_file = new std::ofstream(error_log_absolute_path, std::ifstream::out);
    if (!state->_error_log_file->is_open()) {
        std::stringstream error_msg;
        error_msg << "Fail to open error file: [" << state->_error_log_file_path << "].";
        LOG(WARNING) << error_msg.str();
        return Status::InternalError(error_msg.str());
    }
    return Status::OK();
}

void LoadPathStateHelper::append_error_msg_to_file(RuntimeState* state, const std::string& line,
                                                   const std::string& error_msg, bool is_summary) {
    std::lock_guard<std::mutex> l(state->_error_log_lock);
    if (state->_query_options.query_type != TQueryType::LOAD) {
        return;
    }
    // If file hasn't been opened, open it here.
    if (state->_error_log_file == nullptr) {
        Status status = LoadPathStateHelper::create_error_log_file(state);
        if (!status.ok()) {
            LOG(WARNING) << "Create error file log failed. because: " << status.message();
            if (state->_error_log_file != nullptr) {
                state->_error_log_file->close();
                delete state->_error_log_file;
                state->_error_log_file = nullptr;
            }
            return;
        }
    }

    // If num of printed error row exceeds the limit, and this is not a summary message, return.
    if (state->_num_print_error_rows.fetch_add(1, std::memory_order_relaxed) > kMaxErrorNum && !is_summary) {
        return;
    }

    std::stringstream out;
    if (is_summary) {
        out << "Error: ";
        out << error_msg;
    } else {
        // Note: export reason first in case src line too long and be truncated.
        out << "Error: " << error_msg << ". Row: " << line;
    }

    if (!out.str().empty()) {
        (*state->_error_log_file) << out.str() << std::endl;
    }
}

void LoadPathStateHelper::append_rejected_record_to_file(RuntimeState* state, const std::string& record,
                                                         const std::string& error_msg, const std::string& source) {
    // Only load jobs produce rejected records.
    if (state->_query_options.query_type != TQueryType::LOAD) {
        return;
    }
    auto* writer = LoadPathStateHelper::rejected_record_writer(state);
    if (writer == nullptr) {
        return;
    }

    std::string source_info_json = build_rejected_record_source_info(state->_query_options, source);
    writer->append_raw(record, /*error_code=*/"REJECTED", error_msg, /*error_column=*/"", source_info_json);
}

std::string LoadPathStateHelper::build_rejected_record_source_info(const TQueryOptions& query_options,
                                                                   const std::string& source) {
    // Routine load tasks pre-populate the kafka anchor at submit_task
    // time -- prefer it because `source` here would otherwise be the
    // SequentialFile placeholder name ("stream-load-pipe") which is
    // useless to operators. The thrift field carries pre-serialized JSON
    // built by RoutineLoadTaskExecutor; we forward it verbatim.
    if (query_options.__isset.routine_load_source_info && !query_options.routine_load_source_info.empty()) {
        return query_options.routine_load_source_info;
    }
    if (source.empty()) {
        return std::string();
    }
    // Wrap the free-form `source` string (typically a file path) in JSON
    // so the system table column always carries valid JSON; rapidjson
    // escapes embedded quotes / backslashes correctly.
    rapidjson::Document d;
    d.SetObject();
    auto& alloc = d.GetAllocator();
    d.AddMember("source", rapidjson::Value(source.c_str(), static_cast<rapidjson::SizeType>(source.size()), alloc),
                alloc);
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> w(buf);
    d.Accept(w);
    return std::string(buf.GetString(), buf.GetSize());
}

RejectedRecordWriter* LoadPathStateHelper::rejected_record_writer(RuntimeState* state) {
    if (state == nullptr) {
        return nullptr;
    }
    // Only LOAD queries produce rejected records; short-circuit to avoid
    // allocating a writer for pure SELECT queries that happen to set
    // log_rejected_record_num.
    if (state->_query_options.query_type != TQueryType::LOAD) {
        return nullptr;
    }
    std::lock_guard<std::mutex> l(state->_rejected_record_lock);
    if (state->_rejected_record_writer == nullptr) {
        state->_rejected_record_writer = std::make_shared<RejectedRecordWriter>(state);
    }
    return state->_rejected_record_writer.get();
}

} // namespace starrocks
