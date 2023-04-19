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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/http_common.h

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

#include <string>

namespace starrocks {

static const std::string HTTP_DB_KEY = "db";
static const std::string HTTP_TABLE_KEY = "table";
static const std::string HTTP_TXN_OP_KEY = "txn_op";
static const std::string HTTP_LABEL_KEY = "label";
static const std::string HTTP_FORMAT_KEY = "format";
static const std::string HTTP_COLUMNS = "columns";
static const std::string HTTP_WHERE = "where";
static const std::string HTTP_COLUMN_SEPARATOR = "column_separator";
static const std::string HTTP_ROW_DELIMITER = "row_delimiter";
static const std::string HTTP_SKIP_HEADER = "skip_header";
static const std::string HTTP_TRIM_SPACE = "trim_space";
static const std::string HTTP_ENCLOSE = "enclose";
static const std::string HTTP_ESCAPE = "escape";
static const std::string HTTP_MAX_FILTER_RATIO = "max_filter_ratio";
static const std::string HTTP_TIMEOUT = "timeout";
static const std::string HTTP_IDLE_TRANSACTION_TIMEOUT = "idle_transaction_timeout";
static const std::string HTTP_PARTITIONS = "partitions";
static const std::string HTTP_TEMP_PARTITIONS = "temporary_partitions";
static const std::string HTTP_NEGATIVE = "negative";
static const std::string HTTP_STRICT_MODE = "strict_mode";
static const std::string HTTP_TIMEZONE = "timezone";
static const std::string HTTP_LOAD_MEM_LIMIT = "load_mem_limit";
static const std::string HTTP_EXEC_MEM_LIMIT = "exec_mem_limit";
static const std::string HTTP_JSONPATHS = "jsonpaths";
static const std::string HTTP_JSONROOT = "json_root";
static const std::string HTTP_IGNORE_JSON_SIZE = "ignore_json_size";
static const std::string HTTP_STRIP_OUTER_ARRAY = "strip_outer_array";
static const std::string HTTP_PARTIAL_UPDATE = "partial_update";
static const std::string HTTP_TRANSMISSION_COMPRESSION_TYPE = "transmission_compression_type";
static const std::string HTTP_LOAD_DOP = "load_dop";
static const std::string HTTP_ENABLE_REPLICATED_STORAGE = "enable_replicated_storage";
static const std::string HTTP_MERGE_CONDITION = "merge_condition";
static const std::string HTTP_LOG_REJECTED_RECORD_NUM = "log_rejected_record_num";
static const std::string HTTP_PARTIAL_UPDATE_MODE = "partial_update_mode";

static const std::string HTTP_100_CONTINUE = "100-continue";
static const std::string HTTP_CHANNEL_ID = "channel_id";

} // namespace starrocks
