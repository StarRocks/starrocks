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

#include <boost/algorithm/string.hpp>
#include <orc/OrcFile.hh>

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter_bank.h"
#include "formats/orc/orc_mapping.h"
#include "gen_cpp/orc_proto.pb.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"

namespace starrocks {

class OrcMinMaxDecoder {
public:
    // to decode min and max value from column stats.
    static Status decode(SlotDescriptor* slot, const orc::Type* type, const orc::proto::ColumnStatistics& stats,
                         ColumnPtr min_col, ColumnPtr max_col, int64_t tz_offset_in_seconds);
};

} // namespace starrocks
