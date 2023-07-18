
#include "util/failpoint/fail_point.h"

#include <filesystem>

#include "fmt/format.h"
#include "simdjson.h"

namespace starrocks::failpoint {

int check_fail_point(const char* name, int* failnum, void** failinfo, unsigned int* flags) {
    auto fp = FailPointRegistry::GetInstance()->get(name);
    if (fp == nullptr) {
        LOG(WARNING) << "cannot find failpoint with name " << name;
        return 0;
    }
    return fp->shouldFail();
}

FailPoint::FailPoint(const std::string& name) : _name(name) {
    _trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
}

bool FailPoint::shouldFail() {
    std::shared_lock l(_mu);
    const auto mode = _trigger_mode.mode();
    switch (mode) {
    case FailPointTriggerModeType::ENABLE:
        return true;
    case FailPointTriggerModeType::DISABLE:
        return false;
    case FailPointTriggerModeType::PROBABILITY_ENABLE:
        if (drand48() <= static_cast<double>(_trigger_mode.probability())) {
            return true;
        }
        return false;
    case FailPointTriggerModeType::ENABLE_N_TIMES:
        if (_n_times-- > 0) {
            return true;
        }
        return false;
    default:
        DCHECK(false);
        break;
    }
    return false;
}

void FailPoint::setMode(const PFailPointTriggerMode& p_trigger_mode) {
    LOG(INFO) << "failpoint change mode, name: " << _name << ", mode: " << p_trigger_mode.DebugString();
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
            }
            fp->setMode(trigger_mode);
        }
        return true;
    } catch (...) {
    }
    return false;
}

FailPointRegistry::FailPointRegistry() {
#ifdef FIU_ENABLE
    fiu_init(0);
#endif
}

FailPointRegisterer::FailPointRegisterer(FailPoint* fp) {
    FailPointRegistry::GetInstance()->add(fp);
}

DEFINE_FAIL_POINT(rand_error_during_prepare);

} // namespace starrocks::failpoint