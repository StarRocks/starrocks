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

#pragma once

#include "gen_cpp/segment.pb.h"
#include "storage/olap_common.h"
#include "types/collection.h"
#include "types/type_info.h"

namespace starrocks {

class TabletColumn;
class TypeDescriptor;

// Keep all `get_type_info` entry declarations in storage/types.h for compatibility.
TypeInfoPtr get_type_info(LogicalType field_type);
TypeInfoPtr get_type_info(const ColumnMetaPB& column_meta_pb);
TypeInfoPtr get_type_info(const TabletColumn& col);
TypeInfoPtr get_type_info(const TypeDescriptor& type_desc);
TypeInfoPtr get_type_info(LogicalType field_type, int precision, int scale);
TypeInfoPtr get_type_info(const TypeInfo* type_info);

} // namespace starrocks
