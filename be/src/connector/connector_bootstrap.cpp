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

#include "connector/connector_bootstrap.h"

#include <memory>
#include <string>

#include "connector/connector.h"
#include "connector/connector_registry.h"

#ifdef STARROCKS_WITH_CONNECTOR_BENCHMARK
#include "connector/benchmark/benchmark_connector.h"
#endif

#ifdef STARROCKS_WITH_CONNECTOR_ELASTICSEARCH
#include "connector/elasticsearch/es_connector.h"
#endif

namespace starrocks::connector {

namespace {

template <typename ConnectorT>
void install_if_absent(ConnectorRegistry* registry, const std::string& name) {
    if (registry->get(name) == nullptr) {
        registry->put(name, std::make_unique<ConnectorT>());
    }
}

} // namespace

Status bootstrap_builtin_connectors() {
    auto* registry = ConnectorRegistry::default_instance();
    DCHECK(registry != nullptr);
#ifdef STARROCKS_WITH_CONNECTOR_BENCHMARK
    install_if_absent<BenchmarkConnector>(registry, Connector::BENCHMARK);
#endif
#ifdef STARROCKS_WITH_CONNECTOR_ELASTICSEARCH
    install_if_absent<ESConnector>(registry, Connector::ES);
#endif
    return Status::OK();
}

} // namespace starrocks::connector
