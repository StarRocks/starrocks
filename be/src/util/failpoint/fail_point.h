#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "common/status.h"
#include "common/statusor.h"
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
    std::string _name;
    mutable std::shared_mutex _mu;
    PFailPointTriggerMode _trigger_mode;
    std::atomic_int _n_times = 0;
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

// @TODO need PausableFailPoint?

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

#ifdef FIU_ENABLE
// Use this macro to define failpoint
// NOTE: it can only be used in cpp files, the name of failpoint must be globally unique
#define DEFINE_FAIL_POINT(NAME)                      \
    starrocks::failpoint::FailPoint fp##NAME(#NAME); \
    starrocks::failpoint::FailPointRegisterer fpr##NAME(&fp##NAME);
#define DEFINE_SCOPED_FAIL_POINT(NAME)                      \
    starrocks::failpoint::ScopedFailPoint sfp##NAME(#NAME); \
    starrocks::failpoint::FailPointRegisterer fpr##NAME(&sfp##NAME);
#define FAIL_POINT_SCOPE(NAME) starrocks::failpoint::ScopedFailPointGuard sfpg##NAME(#NAME);
#define FAIL_POINT_TRIGGER_EXECUTE(NAME, stmt) fiu_do_on(#NAME, stmt)
#define FAIL_POINT_TRIGGER_RETURN(NAME, retVal) fiu_return_on(#NAME, retVal)
#define FAIL_POINT_TRIGGER_RETURN_ERROR(NAME) \
    fiu_return_on(#NAME, Status::InternalError(fmt::format("inject error {} at {}:{}", #NAME, __FILE__, __LINE__)))
#else
#define DEFINE_FAIL_POINT(NAME)
#define DEFINE_SCOPED_FAIL_POINT(NAME)
#define FAIL_POINT_SCOPE(NAME)
#define FAIL_POINT_TRIGGER_EXECUTE(NAME, stmt)
#define FAIL_POINT_TRIGGER_RETURN(NAME, retVal)
#define FAIL_POINT_TRIGGER_RETURN_ERROR(NAME)
#endif

bool init_failpoint_from_conf(const std::string& conf_file);

} // namespace starrocks::failpoint
