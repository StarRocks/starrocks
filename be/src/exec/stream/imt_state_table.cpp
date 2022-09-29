// This file is made available under Elastic License 2.0.

#include "exec/stream/imt_state_table.h"

#include "fmt/format.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/descriptors.pb.h"
#include "runtime/descriptors.h"

namespace starrocks {

Status IMTStateTable::init() {
    if (_imt.imt_type != TIMTType::OLAP_TABLE) {
        return Status::NotSupported("only OLAP_TABLE IMT is supported");
    }
    DCHECK(_imt.__isset.olap_table);

    _db_name = _imt.olap_table.db_name;
    _table_name = _imt.olap_table.table_name;

    _table_schema = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(_table_schema->init(_imt.olap_table.schema));
    this->_impl = std::make_unique<vectorized::OlapTablePartitionParam>(_table_schema, _imt.olap_table.partition);
    RETURN_IF_ERROR(this->_impl->init());
    _location = std::make_unique<TOlapTableLocationParam>(_imt.olap_table.location);

    RETURN_IF_ERROR(_init_table_sink());

    return Status::OK();
}

Status IMTStateTable::_init_table_sink() {
    _olap_table_sink_params.load_id.set_hi(_imt.load_id.hi);
    _olap_table_sink_params.load_id.set_lo(_imt.load_id.lo);
    _olap_table_sink_params.txn_id = _imt.txn_id;
    _olap_table_sink_params.txn_trace_parent = "";
    _olap_table_sink_params.num_replicas = 1;
    _olap_table_sink_params.need_gen_rollup = false;
    _olap_table_sink_params.is_lake_table = false;
    _olap_table_sink_params.keys_type = TKeysType::PRIMARY_KEYS;
    _olap_table_sink_params.tuple_id = _imt.olap_table.schema.tuple_desc.id;
    _olap_table_sink_params.nodes_info = _imt.olap_table.nodes_info;;
    _olap_table_sink_params.schema = _imt.olap_table.schema;
    _olap_table_sink_params.partition = _imt.olap_table.partition;
    _olap_table_sink_params.location = _imt.olap_table.location;
    Status status;
    _olap_table_sink = std::make_unique<stream_load::OlapTableSink>(&_pool, _output_exprs, &status);
    RETURN_IF_ERROR(status);
    _olap_table_sink->init(_olap_table_sink_params);
    return Status::OK();
}

Status IMTStateTable::prepare(RuntimeState* state) {
    return _olap_table_sink->prepare(state);
}

Status IMTStateTable::open(RuntimeState* state) {
    return _olap_table_sink->try_open(state);
}

Status IMTStateTable::send_chunk(RuntimeState* state, vectorized::Chunk* chunk) {
    DCHECK(_olap_table_sink);
    return _olap_table_sink->send_chunk(state, chunk);
}

Status IMTStateTable::close(RuntimeState* state) {
    Status status;
    return _olap_table_sink->close(state, status);
}

int64_t IMTStateTable::table_id() const {
    return _table_schema->table_id();
}

const std::string& IMTStateTable::table_name() const {
    return _table_name;
}

std::vector<int64_t> IMTStateTable::get_tablets() const {
    std::vector<int64_t> res;
    for (auto tablet : _location->tablets) {
        res.push_back(tablet.tablet_id);
    }
    return res;
}

std::string IMTStateTable::debug_string() const {
    return fmt::format("IMTStateTable(name={},id={},tablet={}", table_name(), table_id(),
                       fmt::join(get_tablets(), ","));
}

} // namespace starrocks