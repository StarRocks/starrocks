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

#include <cxxabi.h>

#include "util/bthreads/future_impl.h"

namespace starrocks::bthreads {

// Forward declarations.
template <typename R>
class Future;

template <typename R>
class SharedFuture;

template <typename R>
class Promise;

template <typename R>
class FutureBase {
public:
    DISALLOW_COPY(FutureBase);

    bool valid() const noexcept { return static_cast<bool>(_state); }

    void wait() const {
        SharedStateBase::check_state(_state);
        _state->wait();
    }

    template <typename _Rep, typename _Period>
    future_status wait_for(const std::chrono::duration<_Rep, _Period>& rel) const {
        SharedStateBase::check_state(_state);
        return _state->wait_for(rel);
    }

    template <typename _Clock, typename _Duration>
    future_status wait_until(const std::chrono::time_point<_Clock, _Duration>& abs) const {
        SharedStateBase::check_state(_state);
        return _state->wait_until(abs);
    }

    SharedFuture<R> share() { return SharedFuture<R>(std::move(_state)); }

protected:
    constexpr FutureBase() noexcept : _state() {}

    explicit FutureBase(std::shared_ptr<SharedState<R>> state) noexcept : _state(std::move(state)) {}

    void wait_and_check_exception() const {
        if (!static_cast<bool>(_state)) {
            throw future_error(future_errc::no_state);
        }
        this->_state->wait();
        if (this->_state->get_exception_ptr() != nullptr) {
            std::rethrow_exception(this->_state->get_exception_ptr());
        }
    }

    struct Reset {
        explicit Reset(FutureBase& future) noexcept : _future(future) {}
        ~Reset() { _future._state.reset(); }
        FutureBase& _future;
    };

    std::shared_ptr<SharedState<R>> _state;
};

template <typename R>
class Future : public FutureBase<R> {
    static_assert(!std::is_array_v<R>, "result type must not be an array");
    static_assert(!std::is_function_v<R>, "result type must not be a function");
    static_assert(std::is_destructible_v<R>, "result type must be destructible");

    using BaseType = FutureBase<R>;

public:
    constexpr Future() noexcept : BaseType() {}

    /// Move constructor
    Future(Future&& rhs) noexcept : BaseType(std::move(rhs._state)) {}

    DISALLOW_COPY(Future);

    // Move assignment
    Future& operator=(Future&& rhs) noexcept {
        Future(std::move(rhs)).swap(*this);
        return *this;
    }

    void swap(Future& rhs) noexcept { this->_state.swap(rhs._state); }

    R get() {
        typename BaseType::Reset reset(*this);
        SharedStateBase::check_state(this->_state);
        this->wait_and_check_exception();
        return std::move(this->_state->value());
    }

private:
    friend class Promise<R>;

    explicit Future(std::shared_ptr<SharedState<R>> state) : BaseType(std::move(state)) {}
};

template <typename R>
class Future<R&> : public FutureBase<R&> {
    typedef FutureBase<R&> BaseType;

public:
    constexpr Future() noexcept : BaseType() {}

    /// Move constructor
    Future(Future&& rhs) noexcept : BaseType(std::move(rhs._state)) {}

    DISALLOW_COPY(Future);

    // Move assignment
    Future& operator=(Future&& future) noexcept {
        Future(std::move(future)).swap(*this);
        return *this;
    }

    void swap(Future& rhs) noexcept { this->_state.swap(rhs._state); }

    R& get() {
        typename BaseType::Reset reset(*this);
        SharedStateBase::check_state(this->_state);
        this->wait_and_check_exception();
        return this->_state->value();
    }

private:
    friend class Promise<R&>;

    explicit Future(std::shared_ptr<SharedState<R&>> state) : BaseType(std::move(state)) {}
};

template <>
class Future<void> : public FutureBase<void> {
    typedef FutureBase<void> BaseType;

public:
    constexpr Future() noexcept : BaseType() {}

    // Move constructor
    Future(Future&& rhs) noexcept : BaseType(std::move(rhs._state)) {}

    DISALLOW_COPY(Future);

    // Move assignment
    Future& operator=(Future&& future) noexcept {
        Future(std::move(future)).swap(*this);
        return *this;
    }

    void swap(Future& rhs) noexcept { this->_state.swap(rhs._state); }

    void get() {
        typename BaseType::Reset reset(*this);
        this->wait_and_check_exception();
    }

private:
    friend class Promise<void>;

    explicit Future(std::shared_ptr<SharedState<void>> state) : BaseType(std::move(state)) {}
};

template <typename R>
inline void swap(Future<R>& x, Future<R>& y) noexcept {
    x.swap(y);
}

template <typename R>
class SharedFuture : public FutureBase<R> {
    static_assert(!std::is_array_v<R>, "result type must not be an array");
    static_assert(!std::is_function_v<R>, "result type must not be a function");
    static_assert(std::is_destructible_v<R>, "result type must be destructible");

public:
    constexpr SharedFuture() noexcept : BaseType() {}

    // Copy constructor
    SharedFuture(const SharedFuture& rhs) noexcept : BaseType(rhs._state) {}

    // Move constructor
    SharedFuture(SharedFuture&& rhs) noexcept : BaseType(std::move(rhs._state)) {}

    // Copy assignment
    SharedFuture& operator=(const SharedFuture& rhs) {
        this->_state = rhs._state;
        return *this;
    }

    // Move assignment
    SharedFuture& operator=(SharedFuture&& rhs) noexcept {
        SharedFuture(std::move(rhs)).swap(*this);
        return *this;
    }

    // Construct from a Future
    SharedFuture(Future<R>&& future) noexcept : BaseType(std::move(future._state)) {}

    void swap(SharedFuture& rhs) noexcept { this->_state.swap(rhs._state); }

    R get() const {
        SharedStateBase::check_state(this->_state);
        this->wait_and_check_exception();
        return std::move(this->_state->value());
    }

private:
    friend class Promise<R>;
    friend class Future<R>;

    using BaseType = FutureBase<R>;

    explicit SharedFuture(std::shared_ptr<SharedState<R>> state) : BaseType(std::move(state)) {}
};

template <typename R>
class SharedFuture<R&> : public FutureBase<R&> {
    typedef FutureBase<R&> BaseType;

public:
    constexpr SharedFuture() noexcept : BaseType() {}

    // Move constructor
    SharedFuture(SharedFuture&& rhs) noexcept : BaseType(std::move(rhs._state)) {}

    // Copy constructor
    SharedFuture(const SharedFuture& rhs) : BaseType(rhs._state) {}

    // Construct from a Future
    SharedFuture(Future<R&>&& future) : BaseType(std::move(future._state)) {}

    // Copy assignment
    SharedFuture& operator=(const SharedFuture& rhs) {
        this->_state = rhs._state;
        return *this;
    }

    // Move assignment
    SharedFuture& operator=(SharedFuture&& future) noexcept {
        Future(std::move(future)).swap(*this);
        return *this;
    }

    void swap(SharedFuture& rhs) noexcept { this->_state.swap(rhs._state); }

    R& get() const {
        SharedStateBase::check_state(this->_state);
        this->wait_and_check_exception();
        return this->_state->value();
    }

private:
    friend class Promise<R&>;

    explicit SharedFuture(std::shared_ptr<SharedState<R&>> state) : BaseType(std::move(state)) {}
};

template <>
class SharedFuture<void> : public FutureBase<void> {
    typedef FutureBase<void> BaseType;

public:
    constexpr SharedFuture() noexcept : BaseType() {}

    // Move constructor
    SharedFuture(SharedFuture&& rhs) noexcept : BaseType(std::move(rhs._state)) {}

    // Copy constructor
    SharedFuture(const SharedFuture& rhs) : BaseType(rhs._state) {}

    // Construct from a Future
    SharedFuture(Future<void>&& future) : BaseType(std::move(future._state)) {}

    // Assignment
    SharedFuture& operator=(const SharedFuture& rhs) {
        this->_state = rhs._state;
        return *this;
    }

    // Move assignment
    SharedFuture& operator=(SharedFuture&& rhs) noexcept {
        SharedFuture(std::move(rhs)).swap(*this);
        return *this;
    }

    void swap(SharedFuture& rhs) noexcept { _state.swap(rhs._state); }

    /// Retrieving the value
    void get() const {
        SharedStateBase::check_state(this->_state);
        this->wait_and_check_exception();
    }

private:
    friend class Promise<void>;

    explicit SharedFuture(std::shared_ptr<SharedState<void>> state) : BaseType(std::move(state)) {}
};

template <typename R>
inline void swap(SharedFuture<R>& x, SharedFuture<R>& y) noexcept {
    x.swap(y);
}

template <typename R>
class Promise {
    static_assert(!std::is_array_v<R>, "result type must not be an array");
    static_assert(!std::is_function_v<R>, "result type must not be a function");
    static_assert(std::is_destructible_v<R>, "result type must be destructible");

public:
    Promise() : _state(std::make_shared<SharedState<R>>()) {}

    // Move constructor
    Promise(Promise&& rhs) noexcept : _state(std::move(rhs._state)) {}

    DISALLOW_COPY(Promise);

    ~Promise() {
        if (static_cast<bool>(_state) && !_state.unique()) _state->break_promise();
    }

    // Assignment
    Promise& operator=(Promise&& rhs) noexcept {
        Promise(std::move(rhs)).swap(*this);
        return *this;
    }

    void swap(Promise& rhs) noexcept { _state.swap(rhs._state); }

    // Retrieving the result
    Future<R> get_future() {
        SharedStateBase::check_state(_state);
        _state->set_retrieved_flag();
        return Future<R>(_state);
    }

    void set_value(const R& value) {
        SharedStateBase::check_state(this->_state);
        this->_state->set_value(value);
    }

    void set_value(R&& value) {
        SharedStateBase::check_state(this->_state);
        this->_state->set_value(std::move(value));
    }

    void set_exception(std::exception_ptr ex) {
        SharedStateBase::check_state(this->_state);
        this->_state->set_exception(ex);
    }

private:
    std::shared_ptr<SharedState<R>> _state;
};

template <typename R>
class Promise<R&> {
public:
    Promise() : _state(std::make_shared<SharedState<R&>>()) {}

    // Move constructor
    Promise(Promise&& rhs) noexcept : _state(std::move(rhs._state)) {}

    DISALLOW_COPY(Promise);

    ~Promise() {
        if (static_cast<bool>(_state) && !_state.unique()) _state->break_promise();
    }

    // Assignment
    Promise& operator=(Promise&& rhs) noexcept {
        Promise(std::move(rhs)).swap(*this);
        return *this;
    }

    void swap(Promise& rhs) noexcept { _state.swap(rhs._state); }

    Future<R&> get_future() {
        SharedStateBase::check_state(_state);
        _state->set_retrieved_flag();
        return Future<R&>(_state);
    }

    void set_value(R& value) {
        SharedStateBase::check_state(this->_state);
        _state->set_value(value);
    }

    void set_exception(std::exception_ptr e) {
        SharedStateBase::check_state(this->_state);
        _state->set_exception(e);
    }

private:
    std::shared_ptr<SharedState<R&>> _state;
};

template <>
class Promise<void> {
    std::shared_ptr<SharedState<void>> _state;

public:
    Promise() : _state(std::make_shared<SharedState<void>>()) {}

    // Move constructor
    Promise(Promise&& rhs) noexcept : _state(std::move(rhs._state)) {}

    DISALLOW_COPY(Promise);

    ~Promise() {
        if (static_cast<bool>(_state) && !_state.unique()) _state->break_promise();
    }

    // Assignment
    Promise& operator=(Promise&& rhs) noexcept {
        Promise(std::move(rhs)).swap(*this);
        return *this;
    }

    void swap(Promise& rhs) noexcept { _state.swap(rhs._state); }

    Future<void> get_future() {
        SharedStateBase::check_state(_state);
        _state->set_retrieved_flag();
        return Future<void>(_state);
    }

    void set_value() {
        SharedStateBase::check_state(this->_state);
        _state->set_value();
    }

    void set_exception(std::exception_ptr e) {
        SharedStateBase::check_state(this->_state);
        _state->set_exception(e);
    }
};

template <typename R>
inline void swap(Promise<R>& x, Promise<R>& y) noexcept {
    x.swap(y);
}

} // namespace starrocks::bthreads