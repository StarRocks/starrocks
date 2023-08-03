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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/stream_load/stream_load_context.cpp

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

#include "runtime/stream_load/stream_load_context.h"

namespace starrocks {

std::string StreamLoadContext::to_resp_json(const std::string& txn_op, const Status& st) const {
    rapidjson::StringBuffer s;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);

    writer.StartObject();

    writer.Key("Status");
    writer.String(to_string(st.code()).c_str());
    switch (st.code()) {
    case TStatusCode::LABEL_ALREADY_EXISTS:
        writer.Key("ExistingJobStatus");
        writer.String(existing_job_status.c_str());
        break;
    case TStatusCode::TXN_IN_PROCESSING:
        writer.Key("Label");
        writer.String(label.c_str());
        break;
    default:
        break;
    }
    // msg
    writer.Key("Message");
    writer.String(st.get_error_msg().c_str());

    if (st.ok()) {
        // label
        writer.Key("Label");
        writer.String(label.c_str());
        if (boost::iequals(txn_op, TXN_BEGIN)) {
            // txn id
            writer.Key("TxnId");
            writer.Int64(txn_id);
            writer.Key("BeginTxnTimeMs");
            writer.Int64(begin_txn_cost_nanos / 1000000);
        } else if (boost::iequals(txn_op, TXN_COMMIT) || boost::iequals(txn_op, TXN_PREPARE)) {
            // txn id
            writer.Key("TxnId");
            writer.Int64(txn_id);
            // number_load_rows
            writer.Key("NumberTotalRows");
            writer.Int64(number_total_rows);
            writer.Key("NumberLoadedRows");
            writer.Int64(number_loaded_rows);
            writer.Key("NumberFilteredRows");
            writer.Int64(number_filtered_rows);
            writer.Key("NumberUnselectedRows");
            writer.Int64(number_unselected_rows);
            writer.Key("LoadBytes");
            writer.Int64(total_receive_bytes);
            writer.Key("LoadTimeMs");
            writer.Int64(load_cost_nanos / 1000000);
            writer.Key("StreamLoadPlanTimeMs");
            writer.Int64(stream_load_put_cost_nanos / 1000000);
            writer.Key("ReceivedDataTimeMs");
            writer.Int64(total_received_data_cost_nanos / 1000000);
            writer.Key("WriteDataTimeMs");
            writer.Int(write_data_cost_nanos / 1000000);
            writer.Key("CommitAndPublishTimeMs");
            writer.Int64(commit_and_publish_txn_cost_nanos / 1000000);
        } else if (boost::iequals(txn_op, TXN_ROLLBACK)) {
        } else if (boost::iequals(txn_op, TXN_LOAD)) {
            // txn id
            writer.Key("TxnId");
            writer.Int64(txn_id);
            // number_load_rows
            writer.Key("LoadBytes");
            writer.Int64(receive_bytes);
            writer.Key("StreamLoadPlanTimeMs");
            writer.Int64(stream_load_put_cost_nanos / 1000000);
            writer.Key("ReceivedDataTimeMs");
            writer.Int64(received_data_cost_nanos / 1000000);
        }
    }

    if (!error_url.empty()) {
        writer.Key("ErrorURL");
        writer.String(error_url.c_str());
    }
    writer.EndObject();
    return s.GetString();
}

std::string StreamLoadContext::to_json() const {
    rapidjson::StringBuffer s;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);

    writer.StartObject();
    // txn id
    writer.Key("TxnId");
    writer.Int64(txn_id);

    // label
    writer.Key("Label");
    writer.String(label.c_str());

    // status
    writer.Key("Status");
    switch (status.code()) {
    case TStatusCode::OK:
        writer.String("Success");
        break;
    case TStatusCode::PUBLISH_TIMEOUT:
        writer.String("Publish Timeout");
        break;
    case TStatusCode::LABEL_ALREADY_EXISTS:
        writer.String("Label Already Exists");
        writer.Key("ExistingJobStatus");
        writer.String(existing_job_status.c_str());
        break;
    default:
        writer.String("Fail");
        break;
    }
    // msg
    writer.Key("Message");
    if (status.ok()) {
        writer.String("OK");
    } else {
        writer.String(status.get_error_msg().c_str());
    }
    // number_load_rows
    writer.Key("NumberTotalRows");
    writer.Int64(number_total_rows);
    writer.Key("NumberLoadedRows");
    writer.Int64(number_loaded_rows);
    writer.Key("NumberFilteredRows");
    writer.Int64(number_filtered_rows);
    writer.Key("NumberUnselectedRows");
    writer.Int64(number_unselected_rows);
    writer.Key("LoadBytes");
    writer.Int64(receive_bytes);
    writer.Key("LoadTimeMs");
    writer.Int64(load_cost_nanos / 1000000);
    writer.Key("BeginTxnTimeMs");
    writer.Int64(begin_txn_cost_nanos / 1000000);
    writer.Key("StreamLoadPlanTimeMs");
    writer.Int64(stream_load_put_cost_nanos / 1000000);
    writer.Key("ReadDataTimeMs");
    writer.Int64(total_received_data_cost_nanos / 1000000);
    writer.Key("WriteDataTimeMs");
    writer.Int(write_data_cost_nanos / 1000000);
    writer.Key("CommitAndPublishTimeMs");
    writer.Int64(commit_and_publish_txn_cost_nanos / 1000000);

    if (!error_url.empty()) {
        writer.Key("ErrorURL");
        writer.String(error_url.c_str());
    }
    writer.EndObject();
    return s.GetString();
}

std::string StreamLoadContext::brief(bool detail) const {
    std::stringstream ss;
    ss << "id=" << id << ", job_id=" << job_id << ", txn_id: " << txn_id << ", label=" << label << ", db=" << db;
    if (detail) {
        switch (load_src_type) {
        case TLoadSourceType::KAFKA:
            if (kafka_info != nullptr) {
                ss << ", load type: kafka routine load"
                   << ", brokers: " << kafka_info->brokers << ", topic: " << kafka_info->topic << ", partition: ";
                for (auto& entry : kafka_info->begin_offset) {
                    ss << "[" << entry.first << ": " << entry.second << "]";
                }
            }
            break;
        case TLoadSourceType::PULSAR:
            if (pulsar_info != nullptr) {
                ss << ", load type: pulsar routine load"
                   << ", source_url: " << pulsar_info->service_url << ", topic: " << pulsar_info->topic
                   << ", subscription" << pulsar_info->subscription << ", partitions: [";
                for (auto& partition : pulsar_info->partitions) {
                    ss << partition << ",";
                }
                ss << "], initial positions: [";
                for (const auto& [key, value] : pulsar_info->initial_positions) {
                    ss << key << ":" << value << ",";
                }
                ss << "], properties: [";
                for (auto& propertie : pulsar_info->properties) {
                    ss << propertie.first << ": " << propertie.second << ",";
                }
                ss << "].";
            }
            break;
        default:
            break;
        }
    }
    return ss.str();
}

bool StreamLoadContext::check_and_set_http_limiter(ConcurrentLimiter* limiter) {
    _http_limiter_guard.reset(new ConcurrentLimiterGuard());
    return _http_limiter_guard->set_limiter(limiter);
}

} // namespace starrocks
