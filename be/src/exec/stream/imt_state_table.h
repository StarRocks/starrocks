// This file is made available under Elastic License 2.0.

#pragma once

#include "common/status.h"
#include "exec/tablet_info.h"
#include "exec/tablet_sink.h"
#include "exec/vectorized/tablet_info.h"
#include "gen_cpp/Descriptors_types.h"
#include "storage/chunk_helper.h"
#include "storage/table.h"
#include "storage/table_read_view.h"
#include "storage/table_write_view.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"

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

    // TODO: Fetch  schema from _table?
    std::shared_ptr<vectorized::Schema> schema() {
        if (!_schema) {
            auto tablets = get_tablets();
            DCHECK_LT(0, tablets.size());
            auto tablet_id = tablets[0];
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true);
            _schema = std::make_shared<vectorized::Schema>(ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema()));
        }
        return _schema;
    }
    TableReadViewSharedPtr get_table_reader(const TableReadViewParams& params) {
        DCHECK(_table);
        return _table->create_table_read_view(params);
    }

    int64_t table_id() const;
    const std::string& table_name() const;
    const Version& version() const { return _version; }
    std::vector<int64_t> get_tablets() const;
    std::string debug_string() const;

private:
    Status _init_table_sink();

private:
    const TIMTDescriptor& _imt;

    std::string _db_name;
    std::string _table_name;
    std::shared_ptr<OlapTableSchemaParam> _table_schema;
    std::unique_ptr<vectorized::OlapTablePartitionParam> _partition;
    std::unique_ptr<TOlapTableLocationParam> _location;

    ObjectPool _pool;
    std::vector<TExpr> _output_exprs;
    stream_load::OlapTableSinkParams _olap_table_sink_params;
    std::unique_ptr<stream_load::OlapTableSink> _olap_table_sink;
    std::shared_ptr<Table> _table;
    Version _version;
    std::shared_ptr<vectorized::Schema> _schema;
};

} // namespace starrocks
