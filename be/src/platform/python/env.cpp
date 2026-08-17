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

#include "platform/python/env.h"

#include <fmt/format.h>

#include <filesystem>

namespace starrocks {

std::string PythonEnv::get_python_path() const {
    return fmt::format("{}/bin/python3", home);
}

Status PythonEnvRegistry::init(const std::vector<std::string>& envs) {
    for (const auto& env : envs) {
        std::filesystem::path path = env;
        if (!std::filesystem::is_directory(path)) {
            return Status::InvalidArgument(fmt::format("unsupported python env: {} not a directory", env));
        }

        if (!std::filesystem::exists(path / "bin/python3")) {
            return Status::InvalidArgument(fmt::format("unsupported python env: {} not found python", env));
        }

        PythonEnv python_env;
        python_env.home = env;
        _envs[env] = python_env;
    }
    return Status::OK();
}

StatusOr<PythonEnv> PythonEnvRegistry::getDefault() const {
    if (_envs.empty()) {
        return Status::InternalError("not found avaliable env");
    }
    return _envs.begin()->second;
}

PythonEnvRegistry& global_python_env_registry() {
    static PythonEnvRegistry registry;
    return registry;
}

} // namespace starrocks
