// This file is made available under Elastic License 2.0.

#include "exec/stream/imt_olap_table.h"

#include "fmt/format.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/descriptors.pb.h"
#include "runtime/descriptors.h"

namespace starrocks {

Status OlapTableRouteInfo::init(const TIMTDescriptor& imt) {
    if (imt.imt_type != TIMTType::OLAP_TABLE) {
        return Status::NotSupported("only OLAP_TABLE IMT is supported");
    }
    DCHECK(imt.__isset.olap_table);

    _db_name = imt.olap_table.db_name;
    _table_name = imt.olap_table.table_name;

    _table_schema = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(_table_schema->init(imt.olap_table.schema));
    this->_impl = std::make_unique<vectorized::OlapTablePartitionParam>(_table_schema, imt.olap_table.partition);
    RETURN_IF_ERROR(this->_impl->init());

    _location = std::make_unique<TOlapTableLocationParam>(imt.olap_table.location);

    return Status::OK();
}

int64_t OlapTableRouteInfo::table_id() const {
    return _table_schema->table_id();
}

const std::string& OlapTableRouteInfo::table_name() const {
    return _table_name;
}

std::vector<int64_t> OlapTableRouteInfo::get_tablets() const {
    std::vector<int64_t> res;
    for (auto tablet : _location->tablets) {
        res.push_back(tablet.tablet_id);
    }
    return res;
}

std::string OlapTableRouteInfo::debug_string() const {
    return fmt::format("OlapTableRouteInfo(name={},id={},tablet={}", table_name(), table_id(),
                       fmt::join(get_tablets(), ","));
}

} // namespace starrocks