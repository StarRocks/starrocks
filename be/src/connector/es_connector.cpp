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

#include "connector/es_connector.h"

#include "common/logging.h"
#include "exec/es/es_predicate.h"
#include "exec/es/es_query_builder.h"
#include "exec/es/es_scan_reader.h"
#include "exec/es/es_scroll_parser.h"
#include "exec/es/es_scroll_query.h"
#include "exec/exec_node.h"
#include "exprs/expr.h"
#include "storage/chunk_helper.h"

namespace starrocks::connector {

// ================================

DataSourceProviderPtr ESConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                               const TPlanNode& plan_node) const {
    return std::make_unique<ESDataSourceProvider>(scan_node, plan_node);
}

// ================================

ESDataSourceProvider::ESDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _es_scan_node(plan_node.es_scan_node) {}

DataSourcePtr ESDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<ESDataSource>(this, scan_range);
}

const TupleDescriptor* ESDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_es_scan_node.tuple_id);
}

// ================================

ESDataSource::ESDataSource(const ESDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.es_scan_range) {}

std::string ESDataSource::name() const {
    return "ESDataSource";
}

Status ESDataSource::open(RuntimeState* state) {
    _runtime_state = state;
    const TEsScanNode& es_scan_node = _provider->_es_scan_node;
    _properties = es_scan_node.properties;

    if (es_scan_node.__isset.docvalue_context) {
        _docvalue_context = es_scan_node.docvalue_context;
    }

    if (es_scan_node.__isset.fields_context) {
        _fields_context = es_scan_node.fields_context;
    }

    {
        const auto& it = _properties.find(ESScanReader::KEY_TIME_ZONE);
        if (it != _properties.end()) {
            // Use user customer timezone
            _timezone = it->second;
        } else {
            // Use default timezone in StarRocks
            _timezone = _runtime_state->timezone();
        }
    }

    _tuple_desc = state->desc_tbl().get_tuple_descriptor(es_scan_node.tuple_id);
    DCHECK(_tuple_desc != nullptr);

    const std::vector<SlotDescriptor*> slots = _tuple_desc->slots();
    _column_names.reserve(slots.size());
    for (const auto* slot : slots) {
        if (!slot->is_materialized()) {
            continue;
        }
        _column_names.push_back(slot->col_name());
    }

    RETURN_IF_ERROR(_build_conjuncts());
    RETURN_IF_ERROR(_try_skip_constant_conjuncts());
    if (_no_data) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_normalize_conjuncts());
    RETURN_IF_ERROR(_create_scanner());
    _init_counter();
    return Status::OK();
}

int64_t ESDataSource::raw_rows_read() const {
    return _rows_read_number;
}
int64_t ESDataSource::num_rows_read() const {
    return _rows_return_number;
}
int64_t ESDataSource::num_bytes_read() const {
    return _bytes_read;
}
int64_t ESDataSource::cpu_time_spent() const {
    return _cpu_time_ns;
}

Status ESDataSource::_build_conjuncts() {
    Status status = Status::OK();

    size_t conjunct_sz = _conjunct_ctxs.size();
    _predicates.reserve(conjunct_sz);
    _predicate_idx.reserve(conjunct_sz);

    for (int i = 0; i < _conjunct_ctxs.size(); ++i) {
        EsPredicate* predicate = _pool->add(new EsPredicate(_conjunct_ctxs[i], _tuple_desc, _timezone, _pool));
        predicate->set_field_context(_fields_context);
        status = predicate->build_disjuncts_list();
        if (status.ok()) {
            _predicates.push_back(predicate);
            _predicate_idx.push_back(i);
        } else {
            status = predicate->get_es_query_status();
            if (!status.ok()) {
                LOG(WARNING) << status.get_error_msg();
                return status;
            }
        }
    }
    return status;
}

Status ESDataSource::_try_skip_constant_conjuncts() {
    // TODO: skip constant true
    for (auto& _conjunct_ctx : _conjunct_ctxs) {
        if (_conjunct_ctx->root()->is_constant()) {
            // unreachable path
            // The new optimizer will rewrite `where always false` to `EMPTY_SET`
            ASSIGN_OR_RETURN(ColumnPtr value, _conjunct_ctx->evaluate(nullptr));
            DCHECK(value->is_constant());
            if (value->only_null() || value->get(0).get_uint8() == 0) {
                _no_data = true;
            }
        }
    }
    return Status::OK();
}

Status ESDataSource::_normalize_conjuncts() {
    std::vector<bool> validate_res;
    BooleanQueryBuilder::validate(_predicates, &validate_res);
    DCHECK(validate_res.size() == _predicates.size());

    int counter = 0;
    for (int i = 0; i < _predicates.size(); ++i) {
        if (validate_res[i]) {
            _predicate_idx[counter] = _predicate_idx[i];
            _predicates[counter++] = _predicates[i];
        }
    }
    _predicates.erase(_predicates.begin() + counter, _predicates.end());

    for (int i = _predicate_idx.size() - 1; i >= 0; i--) {
        int conjunct_index = _predicate_idx[i];
        _conjunct_ctxs.erase(_conjunct_ctxs.begin() + conjunct_index);
    }
    return Status::OK();
}

static std::string get_host_port(const std::vector<TNetworkAddress>& es_hosts) {
    std::string localhost = BackendOptions::get_localhost();

    TNetworkAddress host = es_hosts[0];
    for (auto& es_host : es_hosts) {
        if (es_host.hostname == localhost) {
            host = es_host;
            break;
        }
    }

    return fmt::format("{}:{}", host.hostname, host.port);
}

Status ESDataSource::_create_scanner() {
    // create scanner.
    const TEsScanRange& es_scan_range = _scan_range;
    _properties[ESScanReader::KEY_INDEX] = es_scan_range.index;
    if (es_scan_range.__isset.type) {
        _properties[ESScanReader::KEY_TYPE] = es_scan_range.type;
    }
    _properties[ESScanReader::KEY_SHARD] = std::to_string(es_scan_range.shard_id);
    _properties[ESScanReader::KEY_BATCH_SIZE] =
            std::to_string(std::min(config::es_index_max_result_window, _runtime_state->chunk_size()));
    _properties[ESScanReader::KEY_HOST_PORT] = get_host_port(es_scan_range.es_hosts);
    // push down limit to Elasticsearch
    // if have conjunct ES can not process, then must not push down limit operator
    if (_conjunct_ctxs.size() == 0 && _read_limit != -1 && _read_limit <= _runtime_state->chunk_size()) {
        _properties[ESScanReader::KEY_TERMINATE_AFTER] = std::to_string(_read_limit);
    }

    bool doc_value_mode = false;
    _properties[ESScanReader::KEY_QUERY] =
            ESScrollQueryBuilder::build(_properties, _column_names, _predicates, _docvalue_context, &doc_value_mode);

    const std::string& host = _properties.at(ESScanReader::KEY_HOST_PORT);
    _es_reader = _pool->add(new ESScanReader(host, _properties, doc_value_mode));
    RETURN_IF_ERROR(_es_reader->open());
    return Status::OK();
}

void ESDataSource::close(RuntimeState* state) {
    if (_es_reader != nullptr) {
        WARN_IF_ERROR(_es_reader->close(), "close es reader failed");
    }
}

void ESDataSource::_init_counter() {
    _read_counter = ADD_COUNTER(_runtime_profile, "ReadCounter", TUnit::UNIT);
    _rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _read_timer = ADD_TIMER(_runtime_profile, "TotalRawReadTime(*)");
    _materialize_timer = ADD_TIMER(_runtime_profile, "MaterializeTupleTime(*)");
}

Status ESDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    // Notice that some variables are not initialized
    // if `_no_data == true` because of short-circuits logic.
    if (_no_data || (_line_eof && _batch_eof)) {
        return Status::EndOfFile("");
    }

    SCOPED_TIMER(_read_timer);
    while (!_batch_eof) {
        RETURN_IF_CANCELLED(state);
        COUNTER_UPDATE(_read_counter, 1);
        if (_line_eof || _es_scroll_parser == nullptr) {
            RETURN_IF_ERROR(_es_reader->get_next(&_batch_eof, _es_scroll_parser));
            _es_scroll_parser->set_params(_tuple_desc, &_docvalue_context, _timezone);
            if (_batch_eof) {
                return Status::EndOfFile("");
            }
        }

        SCOPED_RAW_TIMER(&_cpu_time_ns);
        {
            SCOPED_TIMER(_materialize_timer);
            RETURN_IF_ERROR(_es_scroll_parser->fill_chunk(state, chunk, &_line_eof));
        }

        Chunk* ck = chunk->get();
        if (ck != nullptr) {
            int64_t before = ck->num_rows();
            COUNTER_UPDATE(_rows_read_counter, before);
            _rows_read_number += before;
            _bytes_read += ck->bytes_usage();

            RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, ck));

            int64_t after = ck->num_rows();
            _rows_return_number += after;
        }

        if (!_line_eof) {
            break;
        }
    }

    return Status::OK();
}

} // namespace starrocks::connector
