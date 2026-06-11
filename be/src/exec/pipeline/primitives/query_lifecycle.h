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

#include "exec/pipeline/pipeline_fwd.h"

namespace starrocks::pipeline {

// Owner callback invoked after a fragment has finished all execution groups.
// The shared pointer pins fragment state while owner-side completion work runs.
class QueryLifecycle {
public:
    virtual ~QueryLifecycle() = default;

    virtual void on_fragment_finished(FragmentContextPtr fragment_ctx) = 0;
};

} // namespace starrocks::pipeline
