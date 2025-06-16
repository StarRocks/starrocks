#pragma once

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
    ~FailPoint() = default;

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
    ~ScopedFailPoint() = default;

    virtual bool shouldFail() override;
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

#ifdef FIU_ENABLE
// Use this macro to define failpoint
// NOTE: it can only be used in cpp files, the name of failpoint must be globally unique
#define DEFINE_FAIL_POINT(NAME)                       \
    starrocks::failpoint::FailPoint fp_##NAME(#NAME); \
    starrocks::failpoint::FailPointRegisterer fpr_##NAME(&fp_##NAME);
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
#define DEFINE_SCOPED_FAIL_POINT(NAME)
#define FAIL_POINT_SCOPE(NAME)
#define FAIL_POINT_TRIGGER_EXECUTE(NAME, stmt)
#define FAIL_POINT_TRIGGER_EXECUTE_OR_DEFAULT(NAME, stmt, default_stmt) default_stmt
#define FAIL_POINT_TRIGGER_RETURN(NAME, retVal)
#define FAIL_POINT_TRIGGER_RETURN_ERROR(NAME)
#define FAIL_POINT_TRIGGER_ASSIGN_STATUS_OR_DEFAULT(NAME, status, stmt, default_stmt) status = default_stmt
#endif

bool init_failpoint_from_conf(const std::string& conf_file);

} // namespace starrocks::failpoint
