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
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "common/logging.h"

namespace starrocks::pipeline {
class PipelineDriver;
using ObserverFunc = std::function<void()>;

class PipelineObserver {
public:
    static std::shared_ptr<PipelineObserver> create(ObserverFunc fn) { return std::make_shared<PipelineObserver>(fn); }

    PipelineObserver(const ObserverFunc& fn) {
        _observer_fn = std::move(fn);
        DCHECK(_observer_fn != nullptr);
    }

    void update() {
        DCHECK(_observer_fn != nullptr);
        _observer_fn();
    }

private:
    friend class PipelinePublisher;
    ObserverFunc _observer_fn = nullptr;
};

using PipelineObserverPtr = std::shared_ptr<PipelineObserver>;

class PipelinePublisher {
public:
    void attach(PipelineObserverPtr& observer) {
        std::lock_guard<std::mutex> l(_mutex);
        _observers.emplace_back(observer);
    }

    void detach(PipelineObserverPtr& observer) {
        std::lock_guard<std::mutex> l(_mutex);
        _observers.erase(std::remove(_observers.begin(), _observers.end(), observer), _observers.end());
    }

    void notify() {
        std::lock_guard<std::mutex> l(_mutex);
        for (auto& ob : _observers) {
            ob->update();
        }
    }

private:
    std::mutex _mutex;
    std::vector<std::shared_ptr<PipelineObserver>> _observers;
};

using PipelinePublisherPtr = std::shared_ptr<PipelinePublisher>;

} // namespace starrocks::pipeline
