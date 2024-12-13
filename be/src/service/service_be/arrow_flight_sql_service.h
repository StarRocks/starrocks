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

#include <arrow/array/builder_binary.h>
#include <arrow/flight/server.h>
#include <arrow/flight/sql/server.h>
#include <arrow/flight/types.h>

#include "common/status.h"

namespace starrocks {

class ArrowFlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
public:
    Status start(int port);
    void stop();

    arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoSchemas(
            const arrow::flight::ServerCallContext& context, const arrow::flight::sql::GetDbSchemas& command,
            const arrow::flight::FlightDescriptor& descriptor) override;

    arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
            const arrow::flight::ServerCallContext& context,
            const arrow::flight::sql::StatementQueryTicket& command) override;

private:
    static arrow::Result<std::pair<std::string, std::string>> decode_ticket(const std::string& ticket);

    bool _running = false;
};

} // namespace starrocks
