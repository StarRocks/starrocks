// This file is made available under Elastic License 2.0.

#include "common/status.h"
#include "exec/tablet_info.h"
#include "exec/vectorized/tablet_info.h"
#include "gen_cpp/Descriptors_types.h"

namespace starrocks {

// Route info of an OLAP Table, which could be used to locate a chunk row to tablet
class OlapTableRouteInfo {
public:
    OlapTableRouteInfo() = default;
    ~OlapTableRouteInfo() = default;

    Status init(const TIMTDescriptor& imt);

    int64_t table_id() const;
    const std::string& table_name() const;
    std::vector<int64_t> get_tablets() const;

    std::string debug_string() const;

private:
    std::string _db_name;
    std::string _table_name;
    std::shared_ptr<OlapTableSchemaParam> _table_schema;
    std::unique_ptr<vectorized::OlapTablePartitionParam> _impl;
    std::unique_ptr<TOlapTableLocationParam> _location;
};

} // namespace starrocks