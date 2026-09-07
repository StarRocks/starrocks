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

#include "compute_env/ai/ai_executor.h"

#include <new>
#include <utility>

#include "common/logging.h"
#include "platform/llm/ai_admission_controller.h"
#include "platform/llm/ai_http_client.h"

namespace starrocks {

StatusOr<std::unique_ptr<AIExecutor>> AIExecutor::create(AIRuntimeConfig config) {
    RETURN_IF_ERROR(config.validate());
    const int completion_capacity = ai_completion_capacity(config.max_inflight, config.worker_thread_num);

    try {
        auto clock = std::make_unique<SystemAIClock>();
        auto config_source_or = AIRuntimeConfigSource::create(config);
        if (!config_source_or.ok()) return config_source_or.status();
        auto config_source = std::move(config_source_or).value();
        auto random = std::make_unique<SystemAIRandom>();

        auto control_scheduler_or = AIControlThreadScheduler::create();
        if (!control_scheduler_or.ok()) return control_scheduler_or.status();
        auto control_scheduler = std::move(control_scheduler_or).value();

        auto completion_executor_or =
                AIThreadPoolCompletionExecutor::create(config.worker_thread_num, completion_capacity);
        if (!completion_executor_or.ok()) return completion_executor_or.status();
        auto completion_executor = std::move(completion_executor_or).value();

        auto http_client_or = AIHttpClient::create();
        if (!http_client_or.ok()) return http_client_or.status();
        auto http_client = std::move(http_client_or).value();

        auto admission_controller = std::make_unique<AIAdmissionController>(clock.get(), control_scheduler.get(),
                                                                            config_source.get(), completion_capacity);
        return std::unique_ptr<AIExecutor>(new AIExecutor(completion_capacity, std::move(clock),
                                                          std::move(config_source), std::move(random),
                                                          std::move(control_scheduler), std::move(completion_executor),
                                                          std::move(http_client), std::move(admission_controller)));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("failed to allocate AI executor runtime");
    } catch (...) {
        return Status::InternalError("failed to create AI executor runtime");
    }
}

AIExecutor::AIExecutor(int completion_capacity, std::unique_ptr<SystemAIClock> clock,
                       std::unique_ptr<AIRuntimeConfigSource> config_source, std::unique_ptr<SystemAIRandom> random,
                       std::unique_ptr<AIControlThreadScheduler> control_scheduler,
                       std::unique_ptr<AIThreadPoolCompletionExecutor> completion_executor,
                       std::unique_ptr<AIHttpClient> http_client,
                       std::unique_ptr<AIAdmissionController> admission_controller) noexcept
        : _completion_capacity(completion_capacity),
          _clock(std::move(clock)),
          _config_source(std::move(config_source)),
          _random(std::move(random)),
          _control_scheduler(std::move(control_scheduler)),
          _completion_executor(std::move(completion_executor)),
          _http_client(std::move(http_client)),
          _admission_controller(std::move(admission_controller)) {}

AIExecutor::~AIExecutor() {
    shutdown();
}

Status AIExecutor::_publish_locked(AIRuntimeConfig candidate, bool worker_changed) {
    const int32_t worker_thread_num = candidate.worker_thread_num;
    ASSIGN_OR_RETURN(auto prepared, _config_source->prepare(std::move(candidate)));

    if (worker_changed) {
        RETURN_IF_ERROR(_completion_executor->update_worker_threads(worker_thread_num));
    }

    _config_source->publish(std::move(prepared));
    return Status::OK();
}

Status AIExecutor::_update_integer(ConfigField field, int64_t value) {
    Status update_status;
    bool notify_admission = false;
    try {
        {
            std::lock_guard lock(_lifecycle_mutex);
            if (_lifecycle != Lifecycle::ACCEPTING) {
                return Status::Shutdown("AI executor is stopping");
            }

            AIRuntimeConfig previous = _config_source->snapshot();
            AIRuntimeConfig candidate = previous;
            bool worker_changed = false;
            switch (field) {
            case ConfigField::REQUEST_TIMEOUT_MS:
                candidate.request_timeout_ms = value;
                break;
            case ConfigField::CONNECT_TIMEOUT_MS:
                candidate.connect_timeout_ms = value;
                break;
            case ConfigField::MAX_RESPONSE_BYTES:
                candidate.max_response_bytes = value;
                break;
            case ConfigField::WORKER_THREAD_NUM:
                candidate.worker_thread_num = static_cast<int32_t>(value);
                worker_changed = candidate.worker_thread_num != previous.worker_thread_num;
                break;
            case ConfigField::SUB_CHUNK_SIZE:
                candidate.sub_chunk_size = static_cast<int32_t>(value);
                break;
            case ConfigField::MAX_RETRIES:
                candidate.max_retries = static_cast<int32_t>(value);
                break;
            case ConfigField::MAX_RETRIES_ON_THROTTLE:
                candidate.max_retries_on_throttle = static_cast<int32_t>(value);
                break;
            case ConfigField::RATE_LIMIT_QPS_CHAT:
                candidate.rate_limit_qps_chat = static_cast<int32_t>(value);
                notify_admission = candidate.rate_limit_qps_chat != previous.rate_limit_qps_chat;
                break;
            case ConfigField::MAX_INFLIGHT:
                candidate.max_inflight = static_cast<int32_t>(value);
                notify_admission = candidate.max_inflight != previous.max_inflight;
                break;
            }
            update_status = _publish_locked(std::move(candidate), worker_changed);
            notify_admission = notify_admission && update_status.ok();
        }
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("failed to allocate AI runtime config update");
    }
    if (notify_admission) {
        _admission_controller->notify_limits_changed();
    }
    return update_status;
}

Status AIExecutor::update_request_timeout_ms(int64_t value) {
    return _update_integer(ConfigField::REQUEST_TIMEOUT_MS, value);
}

Status AIExecutor::update_connect_timeout_ms(int64_t value) {
    return _update_integer(ConfigField::CONNECT_TIMEOUT_MS, value);
}

Status AIExecutor::update_max_response_bytes(int64_t value) {
    return _update_integer(ConfigField::MAX_RESPONSE_BYTES, value);
}

Status AIExecutor::update_worker_thread_num(int32_t value) {
    return _update_integer(ConfigField::WORKER_THREAD_NUM, value);
}

Status AIExecutor::update_sub_chunk_size(int32_t value) {
    return _update_integer(ConfigField::SUB_CHUNK_SIZE, value);
}

Status AIExecutor::update_max_retries(int32_t value) {
    return _update_integer(ConfigField::MAX_RETRIES, value);
}

Status AIExecutor::update_max_retries_on_throttle(int32_t value) {
    return _update_integer(ConfigField::MAX_RETRIES_ON_THROTTLE, value);
}

Status AIExecutor::update_on_error(std::string_view value) {
    std::lock_guard lock(_lifecycle_mutex);
    if (_lifecycle != Lifecycle::ACCEPTING) {
        return Status::Shutdown("AI executor is stopping");
    }
    try {
        AIRuntimeConfig previous = _config_source->snapshot();
        AIRuntimeConfig candidate = previous;
        candidate.on_error.assign(value);
        return _publish_locked(std::move(candidate), false);
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("failed to allocate AI runtime config update");
    }
}

Status AIExecutor::update_rate_limit_qps_chat(int32_t value) {
    return _update_integer(ConfigField::RATE_LIMIT_QPS_CHAT, value);
}

Status AIExecutor::update_max_inflight(int32_t value) {
    return _update_integer(ConfigField::MAX_INFLIGHT, value);
}

void AIExecutor::shutdown() {
    std::unique_lock lock(_lifecycle_mutex);
    if (_lifecycle == Lifecycle::STOPPED) return;
    if (_lifecycle == Lifecycle::STOPPING) {
        _stopped.wait(lock, [this] { return _lifecycle == Lifecycle::STOPPED; });
        return;
    }
    _lifecycle = Lifecycle::STOPPING;
    lock.unlock();

    _admission_controller->shutdown();
    _control_scheduler->shutdown_and_drain();
    _http_client->shutdown();
    _completion_executor->shutdown();

    lock.lock();
    _lifecycle = Lifecycle::STOPPED;
    lock.unlock();
    _stopped.notify_all();
}

} // namespace starrocks
