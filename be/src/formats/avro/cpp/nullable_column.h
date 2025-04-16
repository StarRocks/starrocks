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

#include <avrocpp/GenericDatum.hh>
#include <cctz/time_zone.h>

#include "common/status.h"
#include "runtime/types.h"

namespace starrocks {

class Column;

Status add_adaptive_nullable_column(const avro::GenericDatum& datum, const std::string& col_name,
                                    const TypeDescriptor& type_desc, bool invalid_as_null,
                                    const cctz::time_zone& timezone, Column* column);

Status add_nullable_column(const avro::GenericDatum& datum, const std::string& col_name,
                           const TypeDescriptor& type_desc, bool invalid_as_null, const cctz::time_zone& timezone,
                           Column* column);

} // namespace starrocks
