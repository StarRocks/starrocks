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

#include "data_sink/pipeline/pipeline_sink_registry.h"

#include <string>
#include <utility>
#include <vector>

namespace starrocks::pipeline {
namespace {

std::string unknown_sink_type_message(TDataSinkType::type type) {
    return "unknown data sink type " + std::to_string(static_cast<int>(type));
}

std::string join_missing_provider_names(const std::vector<std::string>& names) {
    std::string message;
    for (size_t i = 0; i < names.size(); ++i) {
        if (i != 0) {
            message.append(", ");
        }
        message.append(names[i]);
    }
    return message;
}

} // namespace

PipelineSinkRegistry::PipelineSinkRegistry(std::map<TDataSinkType::type, PipelineSinkProvider> providers)
        : _providers(std::move(providers)) {}

StatusOr<const PipelineSinkProvider*> PipelineSinkRegistry::find(TDataSinkType::type type) const {
    auto it = _providers.find(type);
    if (it != _providers.end()) {
        return &it->second;
    }

    auto type_name = _TDataSinkType_VALUES_TO_NAMES.find(type);
    if (type_name == _TDataSinkType_VALUES_TO_NAMES.end()) {
        return Status::InternalError(unknown_sink_type_message(type));
    }
    return Status::InternalError(std::string("pipeline sink type ") + type_name->second + " is not registered");
}

Status PipelineSinkRegistryBuilder::add(PipelineSinkProvider provider) {
    if (_frozen != nullptr) {
        return Status::InternalError("pipeline sink registry is frozen");
    }

    auto type_name = _TDataSinkType_VALUES_TO_NAMES.find(provider.type);
    if (type_name == _TDataSinkType_VALUES_TO_NAMES.end()) {
        return Status::InvalidArgument(unknown_sink_type_message(provider.type));
    }
    if (provider.name.empty()) {
        return Status::InvalidArgument("pipeline sink provider name must not be empty");
    }
    if (!provider.build) {
        return Status::InvalidArgument("pipeline sink provider " + provider.name + " has no build callback");
    }

    auto existing = _providers.find(provider.type);
    if (existing != _providers.end()) {
        return Status::AlreadyExist(std::string("pipeline sink type ") + type_name->second +
                                    " is already registered by " + existing->second.name + "; cannot register " +
                                    provider.name);
    }

    _providers.emplace(provider.type, std::move(provider));
    return Status::OK();
}

StatusOr<std::shared_ptr<const PipelineSinkRegistry>> PipelineSinkRegistryBuilder::freeze() {
    if (_frozen != nullptr) {
        return _frozen;
    }

    std::vector<std::string> missing_providers;
    for (const auto& [value, name] : _TDataSinkType_VALUES_TO_NAMES) {
        if (_providers.find(static_cast<TDataSinkType::type>(value)) == _providers.end()) {
            missing_providers.emplace_back(name);
        }
    }
    if (!missing_providers.empty()) {
        return Status::InvalidArgument("pipeline sink registry is incomplete; missing providers: " +
                                       join_missing_provider_names(missing_providers));
    }

    _frozen = std::shared_ptr<const PipelineSinkRegistry>(new PipelineSinkRegistry(std::move(_providers)));
    return _frozen;
}

} // namespace starrocks::pipeline
