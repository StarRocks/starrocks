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

#include <memory>
#include <string>
#include <unordered_map>

#include "connector/connector.h"

namespace starrocks::connector {

class ConnectorRegistry {
public:
    static ConnectorRegistry* default_instance();
    const Connector* get(const std::string& name);
    void put(const std::string& name, std::unique_ptr<Connector> connector);

private:
    std::unordered_map<std::string, std::unique_ptr<Connector>> _connectors;
};

} // namespace starrocks::connector
