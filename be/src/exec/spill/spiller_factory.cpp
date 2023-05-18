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

#include "exec/spill/spiller_factory.h"

#include <memory>
#include <mutex>

#include "common/statusor.h"
#include "exec/spill/spiller.h"

namespace starrocks {
namespace spill {
std::shared_ptr<Spiller> SpillerFactory::create(const SpilledOptions& options) {
    std::lock_guard guard(_mutex);
    auto spiller = std::make_shared<Spiller>(options, shared_from_this());
    _spillers.emplace_back(spiller);
    return spiller;
}

SpillerFactoryPtr make_spilled_factory() {
    return std::make_shared<SpillerFactory>();
}
} // namespace spill
} // namespace starrocks