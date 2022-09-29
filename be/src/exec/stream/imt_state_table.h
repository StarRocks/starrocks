// This file is made available under Elastic License 2.0.

#pragma once

#include "common/status.h"
#include "exec/tablet_info.h"
#include "exec/tablet_sink.h"
#include "exec/vectorized/tablet_info.h"
#include "gen_cpp/Descriptors_types.h"
#include "storage/table.h"
#include "storage/table_read_view.h"
#include "storage/table_write_view.h"

namespace starrocks {

class IMTStateTable;

using IMTStateTablePtr = std::shared_ptr<IMTStateTable>;

// Route info of an OLAP Table, which could be used to locate a chunk row to tablet
class IMTStateTable {
public:
    IMTStateTable(const TIMTDescriptor& imt) : _imt(imt) {}

    ~IMTStateTable() = default;

    Status init();
    Status prepare(RuntimeState* state);
    Status open(RuntimeState* state);
    Status close(RuntimeState* state);
    Status send_chunk(RuntimeState* state, vectorized::Chunk* chunk);
    stream_load::OlapTableSink* olap_table_sink() { return _olap_table_sink.get(); };

    int64_t table_id() const;
    const std::string& table_name() const;
    std::vector<int64_t> get_tablets() const;
    std::string debug_string() const;

private:
    Status _init_table_sink();

private:
    const TIMTDescriptor& _imt;

    std::string _db_name;
    std::string _table_name;
    std::shared_ptr<OlapTableSchemaParam> _table_schema;
    std::unique_ptr<vectorized::OlapTablePartitionParam> _impl;
    std::unique_ptr<TOlapTableLocationParam> _location;

    ObjectPool _pool;
    std::vector<TExpr> _output_exprs;
    stream_load::OlapTableSinkParams _olap_table_sink_params;
    std::unique_ptr<stream_load::OlapTableSink> _olap_table_sink;
};

} // namespace starrocks
