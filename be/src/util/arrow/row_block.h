// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/arrow/row_block.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>

#include "common/status.h"

// Convert StarRocks RowBlockV2 to/from Arrow RecordBatch.
// RowBlockV2 is used in StarRocks storage engine.

namespace arrow {

class Schema;
class MemoryPool;
class RecordBatch;

} // namespace arrow

namespace starrocks {

class Schema;

// Convert StarRocks Schema to Arrow Schema.
Status convert_to_arrow_schema(const Schema& row_desc, std::shared_ptr<arrow::Schema>* result);

// Convert Arrow Schema to StarRocks Schema.
Status convert_to_starrocks_schema(const arrow::Schema& schema, std::shared_ptr<Schema>* result);

} // namespace starrocks
