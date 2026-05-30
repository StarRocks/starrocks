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

#include "connector/connector.h"

namespace starrocks::connector {

const std::string Connector::HIVE = "hive";
const std::string Connector::ES = "es";
const std::string Connector::JDBC = "jdbc";
const std::string Connector::MYSQL = "mysql";
const std::string Connector::FILE = "file";
const std::string Connector::LAKE = "lake";
const std::string Connector::BINLOG = "binlog";
const std::string Connector::ICEBERG = "iceberg";
const std::string Connector::BENCHMARK = "benchmark";
const std::string Connector::CACHE_STATS = "cache_stats";

} // namespace starrocks::connector
