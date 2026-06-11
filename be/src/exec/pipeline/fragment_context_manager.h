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

#include <functional>
#include <mutex>
#include <unordered_map>

#include "base/hash/hash_std.hpp"
#include "base/uid_util.h"
#include "common/status.h"
#include "exec/pipeline/pipeline_fwd.h"

namespace starrocks::pipeline {

class FragmentContextManager {
public:
    explicit FragmentContextManager(QueryLifecycle* query_lifecycle);
    ~FragmentContextManager() = default;

    FragmentContextManager(const FragmentContextManager&) = delete;
    FragmentContextManager(FragmentContextManager&&) = delete;
    FragmentContextManager& operator=(const FragmentContextManager&) = delete;
    FragmentContextManager& operator=(FragmentContextManager&&) = delete;

    FragmentContext* get_or_register(const TUniqueId& fragment_id);
    FragmentContextPtr get(const TUniqueId& fragment_id);

    Status register_ctx(const TUniqueId& fragment_id, FragmentContextPtr fragment_ctx);
    void unregister(const TUniqueId& fragment_id);

    void cancel(const Status& status);

    void for_each_fragment(const std::function<void(const FragmentContextPtr&)>& caller);

private:
    void _attach_query_lifecycle(const FragmentContextPtr& fragment_ctx) const;

    std::mutex _lock;
    std::unordered_map<TUniqueId, FragmentContextPtr> _fragment_contexts;
    QueryLifecycle* _query_lifecycle;
};

} // namespace starrocks::pipeline
