#include "exec/schema_scanner/schema_fe_threads_scanner.h"

#include <simdjson.h>

#include <utility>

#include "column/nullable_column.h"
#include "gutil/strings/substitute.h"
#include "http/http_client.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "util/slice.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaFeThreadsScanner::_s_columns[] = {
        {"FE_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"CONNECTION_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"USER", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"HOST", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"DB", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"COMMAND", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"CONNECTION_START_TIME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TIME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"STATE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"INFO", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"IS_PENDING", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"WAREHOUSE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"CNGROUP", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"CATALOG", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"QUERY_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
};

SchemaFeThreadsScanner::SchemaFeThreadsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaFeThreadsScanner::~SchemaFeThreadsScanner() = default;

Status SchemaFeThreadsScanner::start(RuntimeState* state) {
    _rows.clear();
    _cur_idx = 0;
    RETURN_IF_ERROR(_fetch_fe_threads(state));
    return Status::OK();
}

Status SchemaFeThreadsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_cur_idx >= _rows.size()) {
        *eos = true;
        return Status::OK();
    }
    if (chunk == nullptr || eos == nullptr) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = false;
    return fill_chunk(chunk);
}

Status SchemaFeThreadsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    auto end = _cur_idx + 1;
    for (; _cur_idx < end; _cur_idx++) {
        auto& row = _rows[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 15) {
                return Status::InternalError(strings::Substitute("invalid slot id:$0", slot_id));
            }
            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            if (slot_id == 1) {
                Slice v(row.fe_id);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                continue;
            }
            size_t field_idx = slot_id - 2;
            if (field_idx >= THREAD_FIELD_COUNT) {
                return Status::InternalError(strings::Substitute("invalid field index:$0", field_idx));
            }
            if (!row.fields[field_idx].has_value()) {
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                continue;
            }
            Slice v(row.fields[field_idx].value());
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
        }
    }
    return Status::OK();
}

Status SchemaFeThreadsScanner::_fetch_fe_threads(RuntimeState* state) {
    if (_param->frontends.empty()) {
        return Status::InternalError("frontend list is empty");
    }
    auto timeout_ms = state->query_options().query_timeout * 1000 / 2;
    for (const TFrontend& frontend : _param->frontends) {
        std::string threads_payload;
        std::string url =
                strings::Substitute("http://$0:$1/rest/v1/fe_threads?show_full=true", frontend.ip, frontend.http_port);
        auto http_cb = [&url, &threads_payload, timeout_ms](HttpClient* client) {
            RETURN_IF_ERROR(client->init(url));
            client->set_timeout_ms(timeout_ms);
            RETURN_IF_ERROR(client->execute(&threads_payload));
            return Status::OK();
        };
        RETURN_IF_ERROR(HttpClient::execute_with_retry(2 /* retry times */, 1 /* sleep interval */, http_cb));
        RETURN_IF_ERROR(_parse_threads_payload(frontend.id, threads_payload));
    }
    return Status::OK();
}

Status SchemaFeThreadsScanner::_parse_threads_payload(const std::string& fe_id, const std::string& payload) {
    if (payload.empty()) {
        return Status::OK();
    }
    try {
        simdjson::dom::parser parser;
        simdjson::dom::element doc = parser.parse(payload.c_str(), payload.length());
        simdjson::dom::array threads;
        if (doc.is_array()) {
            auto threads_result = doc.get_array();
            if (threads_result.error()) {
                return Status::InternalError(strings::Substitute(
                        "failed to parse threads array from $0: $1", fe_id,
                        simdjson::error_message(threads_result.error())));
            }
            threads = threads_result.value();
        } else {
            auto threads_element = doc["threads"];
            if (threads_element.error()) {
                return Status::InternalError(strings::Substitute(
                        "missing threads field in fe threads response from $0", fe_id));
            }
            auto array_res = threads_element.value().get_array();
            if (array_res.error()) {
                return Status::InternalError(strings::Substitute(
                        "failed to parse threads field as array from $0: $1", fe_id,
                        simdjson::error_message(array_res.error())));
            }
            threads = array_res.value();
        }

        for (simdjson::dom::element thread : threads) {
            auto row_array_result = thread.get_array();
            if (row_array_result.error()) {
                return Status::InternalError(strings::Substitute(
                        "unexpected fe threads element type from $0: $1", fe_id,
                        simdjson::error_message(row_array_result.error())));
            }
            simdjson::dom::array row_array = row_array_result.value();
            ThreadRow row;
            row.fe_id = fe_id;

            size_t idx = 0;
            for (simdjson::dom::element value : row_array) {
                if (idx >= THREAD_FIELD_COUNT) {
                    break;
                }
                if (value.is_null()) {
                    idx++;
                    continue;
                }
                if (value.is_string()) {
                    auto str_res = value.get_string();
                    if (str_res.error()) {
                        auto json_str = simdjson::to_json_string(value);
                        if (json_str.error()) {
                            return Status::InternalError(strings::Substitute(
                                    "failed to stringify fe thread value from $0: $1", fe_id,
                                    simdjson::error_message(json_str.error())));
                        }
                        row.fields[idx] = json_str.value();
                    } else {
                        row.fields[idx] = std::string(str_res.value());
                    }
                } else {
                    auto json_str = simdjson::to_json_string(value);
                    if (json_str.error()) {
                        return Status::InternalError(strings::Substitute(
                                "failed to stringify fe thread value from $0: $1", fe_id,
                                simdjson::error_message(json_str.error())));
                    }
                    row.fields[idx] = json_str.value();
                }
                idx++;
            }
            _rows.emplace_back(std::move(row));
        }
    } catch (const simdjson::simdjson_error& e) {
        return Status::InternalError(strings::Substitute("failed to parse fe threads response from $0: $1", fe_id,
                                                         simdjson::error_message(e.error())));
    }
    return Status::OK();
}

} // namespace starrocks
