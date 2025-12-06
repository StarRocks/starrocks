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

#include "connector/redis_connector.h"

#include "exec/exec_node.h"
#include "exec/redis_scanner.h"

namespace starrocks::connector {

// ================================

DataSourceProviderPtr RedisConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                  const TPlanNode& plan_node) const {
    return std::make_unique<RedisDataSourceProvider>(scan_node, plan_node);
}

// ================================

RedisDataSourceProvider::RedisDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _redis_scan_node(plan_node.redis_scan_node) {}

DataSourcePtr RedisDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<RedisDataSource>(this, scan_range);
}

const TupleDescriptor* RedisDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_redis_scan_node.tuple_id);
}

// ================================

RedisDataSource::RedisDataSource(const RedisDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider) {}

std::string RedisDataSource::name() const {
    return "RedisDataSource";
}

Status RedisDataSource::open(RuntimeState* state) {
    const TRedisScanNode& redis_scan_node = _provider->_redis_scan_node;
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(redis_scan_node.tuple_id);
    RETURN_IF_ERROR(_create_scanner(state));
    return Status::OK();
}

void RedisDataSource::close(RuntimeState* state) {
    if (_scanner != nullptr) {
        WARN_IF_ERROR(_scanner->close(state), "close redis scanner failed");
    }
}

Status RedisDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    bool eos = false;
    RETURN_IF_ERROR(_init_chunk_if_needed(chunk, 0));
    do {
        RETURN_IF_ERROR(_scanner->get_next(state, chunk, &eos));
        if (Chunk* ck = chunk->get(); ck != nullptr) {
            RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, ck));
        }
    } while (!eos && (*chunk)->num_rows() == 0);
    if (eos) {
        return Status::EndOfFile("");
    }
    _rows_read += (*chunk)->num_rows();
    _bytes_read += (*chunk)->bytes_usage();
    return Status::OK();
}

int64_t RedisDataSource::raw_rows_read() const {
    return _rows_read;
}
int64_t RedisDataSource::num_rows_read() const {
    return _rows_read;
}
int64_t RedisDataSource::num_bytes_read() const {
    return _bytes_read;
}
int64_t RedisDataSource::cpu_time_spent() const {
    // TODO: calculte the real cputime
    return 0;
}

Status RedisDataSource::_create_scanner(RuntimeState* state) {
    const TRedisScanNode& redis_scan_node = _provider->_redis_scan_node;
    const auto* redis_table = down_cast<const RedisTableDescriptor*>(_tuple_desc->table_desc());

    RedisScanContext scan_ctx;
    scan_ctx.tbl_name = redis_table->name();
    scan_ctx.db_name = redis_table->database();
    scan_ctx.redis_url = redis_table->redis_url();
    scan_ctx.user = redis_table->redis_user();
    scan_ctx.passwd = redis_table->redis_passwd();
    scan_ctx.column_names = redis_scan_node.columns;
    scan_ctx.value_data_format = redis_table->value_data_format();
    _scanner = _pool->add(new RedisScanner(scan_ctx, _tuple_desc, _runtime_profile));

    RETURN_IF_ERROR(_scanner->open(state));
    return Status::OK();
}

} // namespace starrocks::connector
