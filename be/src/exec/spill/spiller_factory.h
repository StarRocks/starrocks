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

#include <atomic>
#include <memory>
#include <mutex>

#include "exec/sort_exec_exprs.h"
#include "exec/sorting/sorting.h"
#include "exec/spill/spiller_path_provider.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"

namespace starrocks {
struct SpilledOptions;
class Spiller;

class SpillerFactory : public std::enable_shared_from_this<SpillerFactory> {
public:
    SpillerFactory() = default;
    ~SpillerFactory() = default;

    // create a spiller
    std::shared_ptr<Spiller> create(const SpilledOptions& options);

    // release some resource in advance
    void close();

private:
    std::mutex _mutex;
    std::vector<std::shared_ptr<Spiller>> _spillers;
};

using SpillerFactoryPtr = std::shared_ptr<SpillerFactory>;
SpillerFactoryPtr make_spilled_factory();

} // namespace starrocks