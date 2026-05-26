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

#include "compute_env/compute_env.h"

#include <utility>

#include "compute_env/data_stream/data_stream_mgr.h"
#include "compute_env/pipeline/driver_limiter.h"
#include "compute_env/pipeline/pipeline_timer.h"
#include "compute_env/result/result_buffer_mgr.h"
#include "compute_env/result/result_queue_mgr.h"

namespace starrocks {

ComputeEnv::ComputeEnv() = default;

ComputeEnv::~ComputeEnv() = default;

Status ComputeEnv::init(const ComputeEnvOptions& options) {
    auto driver_limiter = std::make_unique<pipeline::DriverLimiter>(options.max_num_pipeline_drivers);
    auto pipeline_timer = std::make_unique<pipeline::PipelineTimer>();
    auto stream_mgr = std::make_unique<DataStreamMgr>(options.metrics);
    auto result_mgr = std::make_unique<ResultBufferMgr>(options.metrics);
    auto result_queue_mgr = std::make_unique<ResultQueueMgr>(options.metrics);
    RETURN_IF_ERROR(pipeline_timer->start());

    _driver_limiter = std::move(driver_limiter);
    _pipeline_timer = std::move(pipeline_timer);
    _stream_mgr = std::move(stream_mgr);
    _result_mgr = std::move(result_mgr);
    _result_queue_mgr = std::move(result_queue_mgr);
    return Status::OK();
}

void ComputeEnv::stop() {
    if (_stream_mgr != nullptr) {
        _stream_mgr->close();
    }
}

Status ComputeEnv::start_result_mgr() {
    return _result_mgr == nullptr ? Status::OK() : _result_mgr->init();
}

void ComputeEnv::stop_result_mgr() {
    if (_result_mgr != nullptr) {
        _result_mgr->stop();
    }
}

void ComputeEnv::destroy() {
    stop_result_mgr();
    _result_queue_mgr.reset();
    _result_mgr.reset();
    _stream_mgr.reset();
    _driver_limiter.reset();
    _pipeline_timer.reset();
}

} // namespace starrocks
