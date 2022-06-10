// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <unordered_map>

#include "common/status.h"

namespace starrocks {

class TTabletSchema;
class TabletSchemaPB;
enum RowsetTypePB : int;

Status convert_t_schema_to_pb_schema(const TTabletSchema& tablet_schema, uint32_t next_unique_id,
                                     const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                                     RowsetTypePB rowset_type, TabletSchemaPB* schema);

} // namespace starrocks