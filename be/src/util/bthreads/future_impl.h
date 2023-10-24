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

#include <bthread/butex.h>

#include <exception>
#include <functional>
#include <future>
#include <optional>

#include "gutil/macros.h"
#include "util/time.h"

namespace starrocks::bthreads {

enum future_status { ready, timeout };

enum class future_errc { future_already_retrieved = 1, promise_already_satisfied, no_state, broken_promise };

extern const std::error_category& future_category() noexcept;
} // namespace starrocks::bthreads

namespace std {

template <>
struct is_error_code_enum<starrocks::bthreads::future_errc> : public true_type {};

inline std::error_code make_error_code(starrocks::bthreads::future_errc e) noexcept {
    return std::error_code{static_cast<int>(e), starrocks::bthreads::future_category()};
}

inline std::error_condition make_error_condition(starrocks::bthreads::future_errc e) noexcept {
    return std::error_condition{static_cast<int>(e), starrocks::bthreads::future_category()};
}

} // namespace std

namespace starrocks::bthreads {

class future_error : public std::system_error {
public:
    explicit future_error(future_errc code) : std::system_error(std::make_error_code(code)) {}

    ~future_error() override = default;
};

template <typename R>
class FutureBase;

// Forward declarations.
template <typename R>
class Future;

template <typename R>
class SharedFuture;

template <typename R>
class Promise;

class SharedStateBase {
public:
    virtual ~SharedStateBase() { bthread::butex_destroy(_status); }

    DISALLOW_COPY_AND_MOVE(SharedStateBase);

    template <typename T>
    static void check_state(const std::shared_ptr<T>& p) {
        if (!static_cast<bool>(p)) throw future_error(future_errc::no_state);
    }

protected:
    enum Status { kNotReady, kWriting, kReady };

    SharedStateBase() {
        _status = bthread::butex_create_checked<butil::atomic<int>>();
        _status->store(Status::kNotReady, butil::memory_order_release);
    }

    std::exception_ptr get_exception_ptr() const { return _exception; }

    void set_exception(std::exception_ptr exception) {
        int expect_status = Status::kNotReady;
        if (_status->compare_exchange_strong(expect_status, Status::kWriting, butil::memory_order_acq_rel)) {
            _exception = exception;
            mark_ready_ant_notify();
        } else {
            throw future_error(future_errc::promise_already_satisfied);
        }
    }

    void wait() {
        int curr_status;
        while ((curr_status = _status->load(butil::memory_order_acquire)) != Status::kReady) {
            (void)bthread::butex_wait(_status, curr_status, nullptr);
        }
    }

    template <typename Rep, typename Period>
    future_status wait_for(const std::chrono::duration<Rep, Period>& dur) {
        if (_status->load(butil::memory_order_acquire) == Status::kReady) return future_status::ready;

        return wait_until(std::chrono::system_clock::now() +
                          std::chrono::ceil<std::chrono::system_clock::duration>(dur));
    }

    template <typename Clock, typename Duration>
    future_status wait_until(const std::chrono::time_point<Clock, Duration>& abs) {
        int curr_status;
        timespec ts = TimespecFromTimePoint(abs);
        while ((curr_status = _status->load(butil::memory_order_acquire)) != Status::kReady) {
            if (bthread::butex_wait(_status, curr_status, &ts) < 0 && errno == ETIMEDOUT) {
                return future_status::timeout;
            }
        }
        return future_status::ready;
    }

    void mark_ready_ant_notify() {
        _status->store(Status::kReady, butil::memory_order_release);
        bthread::butex_wake_all(_status);
    }

    void set_retrieved_flag() {
        if (_retrieved.test_and_set()) throw future_error(future_errc::future_already_retrieved);
    }

    void break_promise() {
        // This function is only called when the last asynchronous result
        // provider is abandoning this shared state, so noone can be
        // trying to make the shared state ready at the same time
        if (_status->load(butil::memory_order_acquire) != Status::kReady) {
            set_exception(std::make_exception_ptr(future_error(future_errc::broken_promise)));
        }
    }

protected:
    butil::atomic<int>* _status{};
    std::atomic_flag _retrieved{};
    std::exception_ptr _exception;
};

template <class R>
class SharedState : public SharedStateBase {
public:
    SharedState() : SharedStateBase(), _result() {}

    DISALLOW_COPY_AND_MOVE(SharedState);

    ~SharedState() override = default;

private:
    friend class Future<R>;
    friend class FutureBase<R>;
    friend class Promise<R>;

    R& value() noexcept { return _result.value(); }

    void set_value(const R& value) {
        int expect_status = Status::kNotReady;
        if (_status->compare_exchange_strong(expect_status, Status::kWriting, butil::memory_order_acq_rel)) {
            try {
                _result = value;
            } catch (...) {
                _exception = std::current_exception();
            }
            mark_ready_ant_notify();
        } else {
            throw future_error(future_errc::promise_already_satisfied);
        }
    }

    void set_value(R&& value) {
        int expect_status = Status::kNotReady;
        if (_status->compare_exchange_strong(expect_status, Status::kWriting, butil::memory_order_acq_rel)) {
            try {
                _result = std::move(value);
            } catch (...) {
                _exception = std::current_exception();
            }
            mark_ready_ant_notify();
        } else {
            throw future_error(future_errc::promise_already_satisfied);
        }
    }

    std::optional<R> _result;
};

template <class R>
class SharedState<R&> : public SharedStateBase {
public:
    SharedState() : SharedStateBase(), _result(nullptr) {}

    DISALLOW_COPY_AND_MOVE(SharedState);

    ~SharedState() override = default;

private:
    friend class Future<R&>;
    friend class FutureBase<R&>;
    friend class Promise<R&>;

    R& value() noexcept { return *_result; }

    void set_value(R& value) {
        int expect_status = Status::kNotReady;
        if (_status->compare_exchange_strong(expect_status, Status::kWriting, butil::memory_order_acq_rel)) {
            _result = &value;
            mark_ready_ant_notify();
        } else {
            throw future_error(future_errc::promise_already_satisfied);
        }
    }

    R* _result;
};
template <>
class SharedState<void> : public SharedStateBase {
public:
    SharedState() : SharedStateBase() {}

    DISALLOW_COPY_AND_MOVE(SharedState);

    ~SharedState() override = default;

private:
    friend class Future<void>;
    friend class FutureBase<void>;
    friend class Promise<void>;

    void set_value() {
        int expect_status = Status::kNotReady;
        if (_status->compare_exchange_strong(expect_status, Status::kWriting, butil::memory_order_acq_rel)) {
            mark_ready_ant_notify();
        } else {
            throw future_error(future_errc::promise_already_satisfied);
        }
    }
};

} // namespace starrocks::bthreads
