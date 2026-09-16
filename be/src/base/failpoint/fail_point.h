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

#ifdef FIU_ENABLE

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <semaphore>
#include <shared_mutex>
#include <unordered_map>

#include "base/status.h"
#include "base/statusor.h"
#include "fiu/fiu-control.h"
#include "fiu/fiu.h"
#include "gen_cpp/internal_service.pb.h"

namespace starrocks::failpoint {

class FailPoint {
public:
    FailPoint(std::string name);
    virtual ~FailPoint() = default;

    virtual bool shouldFail();

    void setMode(const PFailPointTriggerMode& p_trigger_mode);

    std::string name() const { return _name; }

    PFailPointInfo to_pb() const;

protected:
    // Blocks until setMode() moves _mode_generation past |gen|, or |timeout_second| elapses
    // (|timeout_second| <= 0 falls back to the failpoint layer's default).
    // ALWAYS returns false -- a released pause continues normally and never injects.
    bool wait_until_released(uint64_t gen, int32_t timeout_second);

    // Disarm a pause that reached its timeout, but ONLY if it is still the mode installed as of
    // |gen|. An unconditional disarm would race an operator who re-armed the failpoint in the window
    // between the wait expiring and the disarm landing, silently replacing their new mode with
    // DISABLE. Returns true if this call disarmed it.
    bool disarm_expired_pause(uint64_t gen);

    std::string _name;
    mutable std::shared_mutex _mu;
    PFailPointTriggerMode _trigger_mode;
    std::atomic_int _n_times = 0;

    // Bumped by setMode(). A pausing thread reads it together with the mode under _mu, so a
    // setMode() that races that read is observed as "already changed" instead of being waited for.
    std::atomic<uint64_t> _mode_generation = 0;
    // Deliberately std, NOT bthread, primitives -- this looks like the wrong choice for a failpoint
    // that can sit in a brpc handler, so the reason matters:
    //
    // shouldFail() is reached through libfiu's external callback, from inside fiu_fail(). fiu_fail
    // increments a __thread recursion counter and takes a pthread_rwlock read lock BEFORE invoking
    // the callback, and releases both after it returns (libfiu/fiu.c:286,295,350). A bthread that
    // parks here yields its worker and can resume on a DIFFERENT pthread, so the counter would be
    // incremented on one worker and decremented on another -- leaving the first worker permanently
    // above the recursion threshold, which silently disables EVERY failpoint on it for the life of
    // the process -- and the rwlock would be unlocked by a thread that never took it (UB).
    // Blocking the pthread keeps both balanced.
    //
    // The cost is that a paused failpoint pins its thread: park more brpc handlers than the worker
    // pool has threads and ADMIN DISABLE FAILPOINT cannot be served until the pause times out. That
    // is bounded and visible; the bthread alternative is silent and permanent.
    //
    // The wait also has its own mutex so shouldFail() can drop _mu before blocking: blocking while
    // holding _mu (even shared) would deadlock setMode(), and therefore the release RPC.
    // Lock order is always _mu -> _pause_mu.
    std::mutex _pause_mu;
    std::condition_variable _pause_cv;

    std::atomic<int64_t> _trigger_count = 0;
    std::atomic<int64_t> _paused_thread_count = 0;
};

class ScopedFailPoint : public FailPoint {
public:
    ScopedFailPoint(const std::string& name) : FailPoint(name) {}
    ~ScopedFailPoint() override = default;

    bool shouldFail() override;
};

class ScopedFailPointGuard {
public:
    ScopedFailPointGuard(const std::string& name);
    ~ScopedFailPointGuard();

private:
    FailPoint* _sfp = nullptr;
};

using FailPointPtr = std::shared_ptr<FailPoint>;

class FailPointRegistry {
public:
    static FailPointRegistry* GetInstance() {
        static FailPointRegistry s_fp_registry;
        return &s_fp_registry;
    }

    Status add(FailPoint* fp);
    FailPoint* get(const std::string& name);

    void iterate(const std::function<void(FailPoint*)>& callback);

private:
    FailPointRegistry();
    ~FailPointRegistry() = default;

    std::unordered_map<std::string, FailPoint*> _fps;
};

class FailPointRegisterer {
public:
    explicit FailPointRegisterer(FailPoint* fp);
};

// This class ONLY used in failpoint testing
// This primitive is useful in scenarios where the B thread must wait for at least one
// A thread to reach a synchronization point, but subsequent A threads should not be blocked
// once B has arrived or the first A has been released.
class OneToAnyBarrier {
public:
    OneToAnyBarrier() = default;
    ~OneToAnyBarrier() = default;

    void arrive_A() {
        bool first = false;
        {
            std::lock_guard<std::mutex> lock(m_);
            if (!a_triggered_) {
                a_triggered_ = true;
                first = true;
            }
        }
        cv_.notify_all();

        if (first) {
            sem_B_.acquire();
        }
    }

    void arrive_B() {
        {
            std::unique_lock<std::mutex> lock(m_);
            cv_.wait(lock, [&] { return a_triggered_; });
        }

        sem_B_.release();
    }

private:
    std::mutex m_;
    std::condition_variable cv_;
    std::counting_semaphore<1> sem_B_{0};
    bool a_triggered_{};
};

bool init_failpoint_from_conf(const std::string& conf_file);

// Lift PUpdateFailPointStatusRequest's request-level pause discriminator into the trigger mode that
// gets stored. Lives here rather than inline in the brpc handler so it can be unit-tested, and so
// there is exactly one place that knows the request encoding. Uses only
// gen_cpp/internal_service.pb.h, which this header already includes, so it does not breach the
// be/src/base layering rule.
PFailPointTriggerMode trigger_mode_from_request(const PUpdateFailPointStatusRequest& request);

} // namespace starrocks::failpoint
#endif

#ifdef FIU_ENABLE
// Use this macro to define failpoint
// NOTE: it can only be used in cpp files, the name of failpoint must be globally unique
#define DEFINE_FAIL_POINT(NAME)                       \
    starrocks::failpoint::FailPoint fp_##NAME(#NAME); \
    starrocks::failpoint::FailPointRegisterer fpr_##NAME(&fp_##NAME);
#define DECLARE_FAIL_POINT(NAME) extern starrocks::failpoint::FailPoint fp_##NAME;
#define DEFINE_SCOPED_FAIL_POINT(NAME)                       \
    starrocks::failpoint::ScopedFailPoint sfp_##NAME(#NAME); \
    starrocks::failpoint::FailPointRegisterer fpr_##NAME(&sfp_##NAME);
#define FAIL_POINT_SCOPE(NAME) starrocks::failpoint::ScopedFailPointGuard sfpg_##NAME(#NAME);
#define FAIL_POINT_TRIGGER_EXECUTE(NAME, stmt) fiu_do_on(#NAME, stmt)
// Execute stmt if the failpoint is triggered, otherwise execute default_stmt
#define FAIL_POINT_TRIGGER_EXECUTE_OR_DEFAULT(NAME, stmt, default_stmt) \
    do {                                                                \
        bool fp_triggered__ = false;                                    \
        fiu_do_on(#NAME, {                                              \
            fp_triggered__ = true;                                      \
            { stmt; }                                                   \
        });                                                             \
        if (!fp_triggered__) {                                          \
            default_stmt;                                               \
        }                                                               \
    } while (false)
#define FAIL_POINT_TRIGGER_RETURN(NAME, retVal) fiu_return_on(#NAME, retVal)
#define FAIL_POINT_TRIGGER_RETURN_ERROR(NAME) \
    fiu_return_on(#NAME, Status::InternalError(fmt::format("inject error {} at {}:{}", #NAME, __FILE__, __LINE__)))
// Execute the stmt, and assign the result to the status if the failpoint is triggered,
// otherwise assign the result of default_stmt to the status
#define FAIL_POINT_TRIGGER_ASSIGN_STATUS_OR_DEFAULT(NAME, status, stmt, default_stmt) \
    do {                                                                              \
        bool fp_triggered__ = false;                                                  \
        fiu_do_on(#NAME, {                                                            \
            fp_triggered__ = true;                                                    \
            { status = stmt; }                                                        \
        });                                                                           \
        if (!fp_triggered__) {                                                        \
            status = default_stmt;                                                    \
        }                                                                             \
    } while (false)
#else
#define DEFINE_FAIL_POINT(NAME)
#define DECLARE_FAIL_POINT(NAME)
#define DEFINE_SCOPED_FAIL_POINT(NAME)
#define FAIL_POINT_SCOPE(NAME)
#define FAIL_POINT_TRIGGER_EXECUTE(NAME, stmt)
#define FAIL_POINT_TRIGGER_EXECUTE_OR_DEFAULT(NAME, stmt, default_stmt) default_stmt
#define FAIL_POINT_TRIGGER_RETURN(NAME, retVal)
#define FAIL_POINT_TRIGGER_RETURN_ERROR(NAME)
#define FAIL_POINT_TRIGGER_ASSIGN_STATUS_OR_DEFAULT(NAME, status, stmt, default_stmt) status = default_stmt
#endif
