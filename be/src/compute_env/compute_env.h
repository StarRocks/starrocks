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

#include "common/status.h"

namespace starrocks {

namespace pipeline {
class DriverLimiter;
class PipelineTimer;
} // namespace pipeline

struct ComputeEnvOptions {
    int max_num_pipeline_drivers = 0;
};

class ComputeEnv {
public:
    ComputeEnv();
    ~ComputeEnv();

    ComputeEnv(const ComputeEnv&) = delete;
    ComputeEnv& operator=(const ComputeEnv&) = delete;

    Status init(const ComputeEnvOptions& options);
    void stop();
    void destroy();

    pipeline::DriverLimiter* driver_limiter() const { return _driver_limiter.get(); }
    pipeline::PipelineTimer* pipeline_timer() const { return _pipeline_timer.get(); }

private:
    std::unique_ptr<pipeline::DriverLimiter> _driver_limiter;
    std::unique_ptr<pipeline::PipelineTimer> _pipeline_timer;
};

} // namespace starrocks
