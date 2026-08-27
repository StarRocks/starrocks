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

#ifdef FIU_ENABLE

#include "base/failpoint/fail_point.h"

#include <chrono>
#include <filesystem>
#include <limits>
#include <unordered_set>
#include <utility>

#include "base/utility/defer_op.h"
#include "fmt/format.h"
#include "simdjson.h"

namespace starrocks::failpoint {

namespace {
// Fallback for a pause whose arming request carries no pause_timeout_second -- a failpoint armed
// straight through the brpc HTTP endpoint or from the conf file. A local constant rather than an
// FE-style config: be/src/base must not depend on be/src/common (see be/module_boundary_manifest.json).
constexpr int32_t kDefaultPauseTimeoutSecond = 300;

// The one place that knows how a pause is encoded into a stored trigger mode. Shared by the RPC
// path (trigger_mode_from_request) and the conf-file path so the mixed-version safety property --
// an old node must see DISABLE, never ENABLE -- is enforced once rather than asserted twice.
void set_pause_trigger_mode(PFailPointTriggerMode* trigger_mode, int32_t timeout_second) {
    // mode = DISABLE, never a dedicated enum value: proto2 reports an unknown enum as the default
    // ENABLE, so a node predating the pause would inject the fault instead of pausing.
    trigger_mode->set_mode(FailPointTriggerModeType::DISABLE);
    trigger_mode->set_pause(true);
    if (timeout_second > 0) {
        trigger_mode->set_pause_timeout_second(timeout_second);
    }
}
} // namespace

int check_fail_point(const char* name, int* failnum, void** failinfo, unsigned int* flags) {
    auto fp = FailPointRegistry::GetInstance()->get(name);
    if (fp == nullptr) {
        LOG(WARNING) << "cannot find failpoint with name " << name;
        return 0;
    }
    return fp->shouldFail();
}

FailPoint::FailPoint(std::string name) : _name(std::move(name)) {
    _trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
}

bool FailPoint::shouldFail() {
    uint64_t gen = 0;
    int32_t timeout_second = 0;
    {
        std::shared_lock l(_mu);
        // Check the pause flag BEFORE the mode: a pause request carries mode = DISABLE so that a
        // node predating the flag disables rather than enabling (see internal_service.proto).
        if (!_trigger_mode.pause()) {
            bool fired = false;
            switch (_trigger_mode.mode()) {
            case FailPointTriggerModeType::ENABLE:
                fired = true;
                break;
            case FailPointTriggerModeType::DISABLE:
                break;
            case FailPointTriggerModeType::PROBABILITY_ENABLE:
                fired = drand48() <= static_cast<double>(_trigger_mode.probability());
                break;
            case FailPointTriggerModeType::ENABLE_N_TIMES:
                fired = _n_times-- > 0;
                break;
            default:
                DCHECK(false);
                break;
            }
            if (fired) {
                _trigger_count.fetch_add(1, std::memory_order_relaxed);
            }
            return fired;
        }
        // Read the generation together with the mode. Reading it later, under _pause_mu, would let a
        // setMode() landing in the gap look like the CURRENT generation, so the thread would then
        // wait for a further change and sleep until its timeout.
        gen = _mode_generation.load(std::memory_order_relaxed);
        timeout_second = _trigger_mode.pause_timeout_second();
    }
    // _mu is released before blocking -- see the _pause_mu comment in the header.
    return wait_until_released(gen, timeout_second);
}

bool FailPoint::wait_until_released(uint64_t gen, int32_t timeout_second) {
    if (timeout_second <= 0) {
        timeout_second = kDefaultPauseTimeoutSecond;
    }
    bool timed_out = false;
    {
        std::unique_lock<std::mutex> l(_pause_mu);
        if (_mode_generation.load(std::memory_order_relaxed) != gen) {
            // Released between dropping _mu and taking _pause_mu. This thread never parks, so it
            // must not be counted as a fire and must not appear in the gauge.
            return false;
        }
        _trigger_count.fetch_add(1, std::memory_order_relaxed);
        _paused_thread_count.fetch_add(1, std::memory_order_relaxed);
        DeferOp gauge([this] { _paused_thread_count.fetch_sub(1, std::memory_order_relaxed); });

        LOG(INFO) << "failpoint " << _name << " paused, waiting for ADMIN DISABLE FAILPOINT";
        // A condition variable rather than Awaitility's polling loop: a pause runs for
        // pause_timeout_second (300s by default), so polling would wake every interval for the whole
        // window, and release should be immediate once DISABLE lands. wait_for is specified against
        // a steady clock, so a wall-clock adjustment cannot extend or truncate it.
        timed_out = !_pause_cv.wait_for(l, std::chrono::seconds(timeout_second), [this, gen] {
            return _mode_generation.load(std::memory_order_relaxed) != gen;
        });
    }
    if (timed_out) {
        // Disarm, do not merely resume: leaving the failpoint armed would park every NEWLY arriving
        // thread for another full timeout, so a failpoint on a hot path (a publish RPC handler, say)
        // would keep the node wedged indefinitely -- the opposite of the self-healing this timeout
        // exists to provide. Done outside the _pause_mu scope because the disarm takes
        // _mu -> _pause_mu, and conditionally so it cannot clobber a concurrent re-arm.
        if (disarm_expired_pause(gen)) {
            LOG(WARNING) << "failpoint " << _name << " pause timed out after " << timeout_second
                         << "s, disarming it and resuming";
        } else {
            LOG(WARNING) << "failpoint " << _name << " pause timed out after " << timeout_second
                         << "s, resuming; its mode changed concurrently so the current state is left "
                         << "unchanged";
        }
    } else {
        LOG(INFO) << "failpoint " << _name << " pause released";
    }
    // A released pause never injects: the caller continues normally.
    return false;
}

PFailPointTriggerMode trigger_mode_from_request(const PUpdateFailPointStatusRequest& request) {
    PFailPointTriggerMode trigger_mode = request.trigger_mode();
    if (request.pause()) {
        set_pause_trigger_mode(&trigger_mode, request.pause_timeout_second());
    }
    return trigger_mode;
}

bool FailPoint::disarm_expired_pause(uint64_t gen) {
    {
        std::lock_guard l(_mu);
        // Same lock order as setMode: _mu -> _pause_mu.
        std::lock_guard<std::mutex> pl(_pause_mu);
        if (_mode_generation.load(std::memory_order_relaxed) != gen) {
            // The mode changed while this pause was expiring -- re-armed by an operator, disabled, or
            // already disarmed by a sibling waiter. Whatever it is now is the current intent; leave
            // it alone.
            return false;
        }
        _trigger_mode.Clear();
        _trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
        _mode_generation.fetch_add(1, std::memory_order_relaxed);
    }
    // Wake any sibling threads parked on the same generation so they resume now rather than each
    // waiting out its own timeout.
    _pause_cv.notify_all();
    return true;
}

void FailPoint::setMode(const PFailPointTriggerMode& p_trigger_mode) {
    LOG(INFO) << "failpoint change mode, name: " << _name << ", mode: " << p_trigger_mode.DebugString();
    {
        std::lock_guard l(_mu);
        _trigger_mode = p_trigger_mode;
        auto type = p_trigger_mode.mode();
        switch (type) {
        case FailPointTriggerModeType::ENABLE_N_TIMES:
            _n_times = p_trigger_mode.n_times();
            break;
        default:
            break;
        }
        // Bump under _pause_mu too. A waiter evaluates the predicate while holding _pause_mu, and a
        // bump plus notify landing inside that window would be lost -- the waiter would then sleep
        // until its timeout. Taking _pause_mu here serialises against that window. _mu -> _pause_mu
        // is the only lock order used; the waiter never holds both.
        std::lock_guard<std::mutex> pl(_pause_mu);
        _mode_generation.fetch_add(1, std::memory_order_relaxed);
    }
    _pause_cv.notify_all();
}

PFailPointInfo FailPoint::to_pb() const {
    std::shared_lock l(_mu);
    PFailPointInfo result;
    result.set_name(_name);
    result.mutable_trigger_mode()->CopyFrom(_trigger_mode);
    result.set_trigger_count(_trigger_count.load(std::memory_order_relaxed));
    result.set_paused_thread_count(_paused_thread_count.load(std::memory_order_relaxed));
    return result;
}

inline thread_local std::unordered_set<FailPoint*> scoped_fail_point_set;

bool ScopedFailPoint::shouldFail() {
    if (scoped_fail_point_set.count(this) == 0) {
        return false;
    }
    bool should_fail = FailPoint::shouldFail();
    if (should_fail) {
        scoped_fail_point_set.erase(this);
    }
    return should_fail;
}

ScopedFailPointGuard::ScopedFailPointGuard(const std::string& name) {
    auto fp = FailPointRegistry::GetInstance()->get(name);
    DCHECK(fp != nullptr) << "failpoint " << name << " not found";
    if (scoped_fail_point_set.find(fp) == scoped_fail_point_set.end()) {
        scoped_fail_point_set.insert(fp);
        _sfp = fp;
    }
}

ScopedFailPointGuard::~ScopedFailPointGuard() {
    if (_sfp) {
        scoped_fail_point_set.erase(_sfp);
        _sfp = nullptr;
    }
}

Status FailPointRegistry::add(FailPoint* fp) {
    auto name = fp->name();
    if (_fps.find(name) != _fps.end()) {
        return Status::AlreadyExist(fmt::format("failpoint {} already exists", name));
    }
    _fps.insert({name, fp});
    // fiu provides fiu_enable/fiu_disable/fiu_enable_random to control the behavior of failpoint.
    // because we add the sematics of ENABLE_N_TIMES and need to implement the trigger condition by ourselves.
    // For convenience, ENABLE/DISABLE/PROBABILITY_ENABLE are also implemented in fiu_enable_external by ourselves.
    // If there is a performance problem here, we can switch back to fiu's own interfaces one by one.
    fiu_enable_external(name.c_str(), 1, nullptr, 0, check_fail_point);
    return Status::OK();
}

FailPoint* FailPointRegistry::get(const std::string& name) {
    auto iter = _fps.find(name);
    if (iter == _fps.end()) {
        return nullptr;
    }
    return iter->second;
}

void FailPointRegistry::iterate(const std::function<void(FailPoint*)>& callback) {
    for (const auto& [_, fp] : _fps) {
        callback(fp);
    }
}

bool init_failpoint_from_conf(const std::string& conf_file) {
    if (!std::filesystem::exists(conf_file.c_str())) {
        return true;
    }
    try {
        LOG(INFO) << "load failpoint from config file: " << conf_file;
        // load file
        simdjson::ondemand::parser parser;
        auto json = simdjson::padded_string::load(conf_file.c_str());
        simdjson::ondemand::document doc = parser.iterate(json);
        auto object = doc.get_object();
        for (auto field : object) {
            auto fp_name = field.unescaped_key();
            if (fp_name.error() != simdjson::SUCCESS) {
                LOG(WARNING) << "cannot parse json key from config file";
                return false;
            }
            auto fp = FailPointRegistry::GetInstance()->get(std::string(fp_name.value()));
            if (fp == nullptr) {
                LOG(WARNING) << "cannot find failpoint with name " << fp_name.value();
                return false;
            }
            auto value = field.value();
            auto mode = value["mode"].get_string();
            if (mode.error() != simdjson::SUCCESS) {
                return false;
            }
            PFailPointTriggerMode trigger_mode;
            if (mode.value() == "enable") {
                trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
            } else if (mode.value() == "enable_n_times") {
                auto n_times = value["n_times"].get_int64();
                if (n_times.error() != simdjson::SUCCESS) {
                    return false;
                }
                trigger_mode.set_mode(FailPointTriggerModeType::ENABLE_N_TIMES);
                trigger_mode.set_n_times(n_times.value());
            } else if (mode.value() == "probability_enable") {
                auto probability = value["probability"].get_double();
                if (probability.error() != simdjson::SUCCESS) {
                    return false;
                }
                trigger_mode.set_mode(FailPointTriggerModeType::PROBABILITY_ENABLE);
                trigger_mode.set_probability(probability.value());
            } else if (mode.value() == "pause") {
                // Optional: absent or out of range leaves the field unset and the failpoint layer
                // falls back to kDefaultPauseTimeoutSecond.
                int32_t timeout_second = 0;
                auto pause_timeout_second = value["pause_timeout_second"].get_int64();
                if (pause_timeout_second.error() == simdjson::SUCCESS) {
                    const int64_t raw = pause_timeout_second.value();
                    if (raw <= 0 || raw > std::numeric_limits<int32_t>::max()) {
                        LOG(WARNING) << "ignoring out-of-range pause_timeout_second " << raw << " for failpoint "
                                     << std::string(fp_name.value());
                    } else {
                        timeout_second = static_cast<int32_t>(raw);
                    }
                }
                set_pause_trigger_mode(&trigger_mode, timeout_second);
            } else {
                // Without this branch trigger_mode stays default-constructed, and proto2 reports an
                // unset optional enum as its first value -- ENABLE -- so a typo here would silently
                // ARM the failpoint instead of being reported. Bail like every other parse failure in
                // this loop does.
                LOG(WARNING) << "unknown failpoint mode \"" << mode.value() << "\" for failpoint "
                             << std::string(fp_name.value());
                return false;
            }
            fp->setMode(trigger_mode);
        }
        return true;
    } catch (...) {
    }
    return false;
}

// NOLINTNEXTLINE(modernize-use-equals-default)
FailPointRegistry::FailPointRegistry() {
#ifdef FIU_ENABLE
    fiu_init(0);
#endif
}

FailPointRegisterer::FailPointRegisterer(FailPoint* fp) {
    (void)FailPointRegistry::GetInstance()->add(fp);
}

DEFINE_FAIL_POINT(random_error);
DEFINE_FAIL_POINT(output_stream_io_error);

} // namespace starrocks::failpoint

#endif
