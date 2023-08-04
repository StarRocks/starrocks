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
//
// <future> -*- C++ -*-
//
// Copyright (C) 2009-2021 Free Software Foundation, Inc.
//
// This file is part of the GNU ISO C++ Library.  This library is free
// software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the
// Free Software Foundation; either version 3, or (at your option)
// any later version.

// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// Under Section 7 of GPL version 3, you are granted additional
// permissions described in the GCC Runtime Library Exception, version
// 3.1, as published by the Free Software Foundation.

// You should have received a copy of the GNU General Public License and
// a copy of the GCC Runtime Library Exception along with this program;
// see the files COPYING3 and COPYING.RUNTIME respectively.  If not, see
// <http://www.gnu.org/licenses/>.
//
// original source location refer to:
//  https://github.com/gcc-mirror/gcc/blob/master/libstdc%2B%2B-v3/include/std/future

#pragma once

#include <bthread/butex.h>
#include <butil/thread_local.h>

#include <condition_variable>
#include <exception>
#include <functional>
#include <future>
#include <mutex>

#include "util/time.h"

namespace starrocks::bthreads {

// Forward declarations.
template <typename _Res>
class future;

template <typename _Res>
class shared_future;

template <typename _Signature>
class packaged_task;

template <typename _Res>
class promise;

/// Base class and enclosing scope.
struct __future_base {
    /// Base class for results.
    struct _Result_base {
        std::exception_ptr _M_error;

        _Result_base(const _Result_base&) = delete;
        _Result_base& operator=(const _Result_base&) = delete;

        // _M_destroy() allows derived classes to control deallocation
        virtual void _M_destroy() = 0;

        struct _Deleter {
            void operator()(_Result_base* __fr) const { __fr->_M_destroy(); }
        };

    protected:
        _Result_base() noexcept = default;
        virtual ~_Result_base() = default;
    };

    /// A unique_ptr for result objects.
    template <typename _Res>
    using _Ptr = std::unique_ptr<_Res, _Result_base::_Deleter>;

    /// A result object that has storage for an object of type _Res.
    template <typename _Res>
    struct _Result : _Result_base {
    private:
        typename std::aligned_storage<sizeof(_Res), alignof(_Res)>::type _M_storage;
        bool _M_initialized;

    public:
        typedef _Res result_type;

        _Result() noexcept : _M_initialized() {}

        ~_Result() override {
            if (_M_initialized) _M_value().~_Res();
        }

        // Return lvalue, future will add const or rvalue-reference
        _Res& _M_value() noexcept { return *static_cast<_Res*>(static_cast<void*>(&_M_storage)); }

        void _M_set(const _Res& __res) {
            ::new (static_cast<void*>(&_M_storage)) _Res(__res);
            _M_initialized = true;
        }

        void _M_set(_Res&& __res) {
            ::new (static_cast<void*>(&_M_storage)) _Res(std::move(__res));
            _M_initialized = true;
        }

    private:
        void _M_destroy() { delete this; }
    };

    // Keep it simple for std::allocator.
    template <typename _Res, typename _Tp>
    static _Ptr<_Result<_Res>> _S_allocate_result(const std::allocator<_Tp>& __a) {
        return _Ptr<_Result<_Res>>(new _Result<_Res>);
    }

    // Base class for various types of shared state created by an
    // asynchronous provider (such as a std::promise) and shared with one
    // or more associated futures.
    class _State_baseV2 {
        typedef _Ptr<_Result_base> _Ptr_type;

        enum _Status : unsigned { __not_ready, __ready };

        _Ptr_type _M_result;
        butil::atomic<int>* _M_status;
        std::atomic_flag _M_retrieved{};
        std::once_flag _M_once;

    public:
        _State_baseV2() noexcept : _M_result(), _M_status(nullptr), _M_once() {
            _M_status = bthread::butex_create_checked<butil::atomic<int>>();
            _M_status->store(_Status::__not_ready, butil::memory_order_release);
        }
        _State_baseV2(const _State_baseV2&) = delete;
        _State_baseV2& operator=(const _State_baseV2&) = delete;
        virtual ~_State_baseV2() { bthread::butex_destroy(_M_status); };

        _Result_base& wait() {
            // Run any deferred function or join any asynchronous thread:
            _M_complete_async();
            // Acquire MO makes sure this synchronizes with the thread that made
            // the future ready.
            while (_M_status->load(butil::memory_order_acquire) != _Status::__ready) {
                bthread::butex_wait(_M_status, _Status::__not_ready, nullptr);
            }
            return *_M_result;
        }

        template <typename _Rep, typename _Period>
        std::future_status wait_for(const std::chrono::duration<_Rep, _Period>& __rel) {
            // First, check if the future has been made ready.  Use acquire MO
            // to synchronize with the thread that made it ready.
            if (_M_status->load(butil::memory_order_acquire) == _Status::__ready) return std::future_status::ready;

            if (_M_is_deferred_future()) return std::future_status::deferred;

            // Don't wait unless the relative time is greater than zero.
            if (__rel > __rel.zero()) {
                using __dur = typename std::chrono::steady_clock::duration;
                return wait_until(std::chrono::steady_clock::now() + std::chrono::ceil<__dur>(__rel));
            }
            return std::future_status::timeout;
        }

        template <typename _Clock, typename _Duration>
        std::future_status wait_until(const std::chrono::time_point<_Clock, _Duration>& __abs) {
            static_assert(std::chrono::is_clock_v<_Clock>);
            // First, check if the future has been made ready.  Use acquire MO
            // to synchronize with the thread that made it ready.
            if (_M_status->load(butil::memory_order_acquire) == _Status::__ready) return std::future_status::ready;

            if (_M_is_deferred_future()) return std::future_status::deferred;

            timespec ts = TimespecFromTimePoint(__abs);
            do {
                if (bthread::butex_wait(_M_status, _Status::__not_ready, &ts) < 0 && errno == ETIMEDOUT)
                    return std::future_status::timeout;
            } while (_M_status->load(butil::memory_order_acquire) != _Status::__ready);
            _M_complete_async();
            return std::future_status::ready;
        }

        // Provide a result to the shared state and make it ready.
        // Calls at most once: _M_result = __res();
        void _M_set_result(std::function<_Ptr_type()> __res, bool __ignore_failure = false) {
            bool __did_set = false;
            // all calls to this function are serialized,
            // side-effects of invoking __res only happen once
            call_once(_M_once, &_State_baseV2::_M_do_set, this, std::addressof(__res), std::addressof(__did_set));
            if (__did_set) {
                // Use release MO to synchronize with observers of the ready state.
                _M_status->store(_Status::__ready, butil::memory_order_release);
                bthread::butex_wake_all(_M_status);
            } else if (!__ignore_failure)
                throw std::future_error(std::future_errc::promise_already_satisfied);
        }

        // Provide a result to the shared state but delay making it ready
        // until the calling thread exits.
        // Calls at most once: _M_result = __res();
        void _M_set_delayed_result(std::function<_Ptr_type()> __res, std::weak_ptr<_State_baseV2> __self) {
            bool __did_set = false;
            std::unique_ptr<_Make_ready> __mr{new _Make_ready};
            // all calls to this function are serialized,
            // side-effects of invoking __res only happen once
            call_once(_M_once, &_State_baseV2::_M_do_set, this, std::addressof(__res), std::addressof(__did_set));
            if (!__did_set) throw std::future_error(std::future_errc::promise_already_satisfied);
            __mr->_M_shared_state = std::move(__self);
            CHECK_EQ(0, butil::thread_atexit(&_Make_ready::_S_run, __mr.release()));
        }

        // Abandon this shared state.
        void _M_break_promise(_Ptr_type __res) {
            if (static_cast<bool>(__res)) {
                __res->_M_error = std::make_exception_ptr(std::future_error(std::future_errc::broken_promise));
                // This function is only called when the last asynchronous result
                // provider is abandoning this shared state, so noone can be
                // trying to make the shared state ready at the same time, and
                // we can access _M_result directly instead of through call_once.
                _M_result.swap(__res);
                // Use release MO to synchronize with observers of the ready state.
                _M_status->store(_Status::__ready, butil::memory_order_release);
                bthread::butex_wake_all(_M_status);
            }
        }

        // Called when this object is first passed to a future.
        void _M_set_retrieved_flag() {
            if (_M_retrieved.test_and_set()) throw std::future_error(std::future_errc::future_already_retrieved);
        }

        template <typename _Res, typename _Arg>
        struct _Setter;

        // set lvalues
        template <typename _Res, typename _Arg>
        struct _Setter<_Res, _Arg&> {
            // check this is only used by promise<R>::set_value(const R&)
            // or promise<R&>::set_value(R&)
            static_assert(std::is_same<_Res, _Arg&>::value                  // promise<R&>
                                  || std::is_same<const _Res, _Arg>::value, // promise<R>
                          "Invalid specialisation");

            // Used by std::promise to copy construct the result.
            typename promise<_Res>::_Ptr_type operator()() const {
                _M_promise->_M_storage->_M_set(*_M_arg);
                return std::move(_M_promise->_M_storage);
            }
            promise<_Res>* _M_promise;
            _Arg* _M_arg;
        };

        // set rvalues
        template <typename _Res>
        struct _Setter<_Res, _Res&&> {
            // Used by std::promise to move construct the result.
            typename promise<_Res>::_Ptr_type operator()() const {
                _M_promise->_M_storage->_M_set(std::move(*_M_arg));
                return std::move(_M_promise->_M_storage);
            }
            promise<_Res>* _M_promise;
            _Res* _M_arg;
        };

        // set void
        template <typename _Res>
        struct _Setter<_Res, void> {
            static_assert(std::is_void<_Res>::value, "Only used for promise<void>");

            typename promise<_Res>::_Ptr_type operator()() const { return std::move(_M_promise->_M_storage); }

            promise<_Res>* _M_promise;
        };

        struct __exception_ptr_tag {};

        // set exceptions
        template <typename _Res>
        struct _Setter<_Res, __exception_ptr_tag> {
            // Used by std::promise to store an exception as the result.
            typename promise<_Res>::_Ptr_type operator()() const {
                _M_promise->_M_storage->_M_error = *_M_ex;
                return std::move(_M_promise->_M_storage);
            }

            promise<_Res>* _M_promise;
            std::exception_ptr* _M_ex;
        };

        template <typename _Res, typename _Arg>
        __attribute__((__always_inline__)) static _Setter<_Res, _Arg&&> __setter(promise<_Res>* __prom,
                                                                                 _Arg&& __arg) noexcept {
            return _Setter<_Res, _Arg&&>{__prom, std::addressof(__arg)};
        }

        template <typename _Res>
        __attribute__((__always_inline__)) static _Setter<_Res, __exception_ptr_tag> __setter(
                std::exception_ptr& __ex, promise<_Res>* __prom) noexcept {
            return _Setter<_Res, __exception_ptr_tag>{__prom, &__ex};
        }

        template <typename _Res>
        __attribute__((__always_inline__)) static _Setter<_Res, void> __setter(promise<_Res>* __prom) noexcept {
            return _Setter<_Res, void>{__prom};
        }

        template <typename _Tp>
        static void _S_check(const std::shared_ptr<_Tp>& __p) {
            if (!static_cast<bool>(__p)) throw std::future_error(std::future_errc::no_state);
        }

    private:
        // The function invoked with std::call_once(_M_once, ...).
        void _M_do_set(std::function<_Ptr_type()>* __f, bool* __did_set) {
            _Ptr_type __res = (*__f)();
            // Notify the caller that we did try to set; if we do not throw an
            // exception, the caller will be aware that it did set (e.g., see
            // _M_set_result).
            *__did_set = true;
            _M_result.swap(__res); // nothrow
        }

        // Wait for completion of async function.
        virtual void _M_complete_async() {}

        // Return true if state corresponds to a deferred function.
        virtual bool _M_is_deferred_future() const { return false; }

        struct _Make_ready {
            std::weak_ptr<_State_baseV2> _M_shared_state;

            static void _S_run(void* arg) {
                _Make_ready* _make_ready = reinterpret_cast<_Make_ready*>(arg);
                std::shared_ptr<_State_baseV2> state = _make_ready->_M_shared_state.lock();
                if (static_cast<bool>(state)) {
                    // Use release MO to synchronize with observers of the ready state.
                    state->_M_status->store(_Status::__ready, butil::memory_order_release);
                    bthread::butex_wake_all(state->_M_status);
                }
                delete _make_ready;
            }
        };
    };

    using _State_base = _State_baseV2;

    template <typename _Signature>
    class _Task_state_base;

    template <typename _Fn, typename _Alloc, typename _Signature>
    class _Task_state;

    template <typename _Res_ptr, typename _Fn, typename _Res = typename _Res_ptr::element_type::result_type>
    struct _Task_setter;

    template <typename _Res_ptr, typename _BoundFn>
    static _Task_setter<_Res_ptr, _BoundFn> _S_task_setter(_Res_ptr& __ptr, _BoundFn& __call) {
        return {std::addressof(__ptr), std::addressof(__call)};
    }
};

/// Partial specialization for reference types.
template <typename _Res>
struct __future_base::_Result<_Res&> : __future_base::_Result_base {
    typedef _Res& result_type;

    _Result() noexcept : _M_value_ptr() {}

    void _M_set(_Res& __res) noexcept { _M_value_ptr = std::addressof(__res); }

    _Res& _M_get() noexcept { return *_M_value_ptr; }

private:
    _Res* _M_value_ptr;

    void _M_destroy() override { delete this; }
};

/// Explicit specialization for void.
template <>
struct __future_base::_Result<void> : __future_base::_Result_base {
    typedef void result_type;

private:
    void _M_destroy() override { delete this; }
};

/// Common implementation for future and shared_future.
template <typename _Res>
class __basic_future : public __future_base {
protected:
    typedef std::shared_ptr<_State_base> __state_type;
    typedef __future_base::_Result<_Res>& __result_type;

private:
    __state_type _M_state;

public:
    // Disable copying.
    __basic_future(const __basic_future&) = delete;
    __basic_future& operator=(const __basic_future&) = delete;

    bool valid() const noexcept { return static_cast<bool>(_M_state); }

    void wait() const {
        _State_base::_S_check(_M_state);
        _M_state->wait();
    }

    template <typename _Rep, typename _Period>
    std::future_status wait_for(const std::chrono::duration<_Rep, _Period>& __rel) const {
        _State_base::_S_check(_M_state);
        return _M_state->wait_for(__rel);
    }

    template <typename _Clock, typename _Duration>
    std::future_status wait_until(const std::chrono::time_point<_Clock, _Duration>& __abs) const {
        _State_base::_S_check(_M_state);
        return _M_state->wait_until(__abs);
    }

protected:
    /// Wait for the state to be ready and rethrow any stored exception
    __result_type _M_get_result() const {
        _State_base::_S_check(_M_state);
        _Result_base& __res = _M_state->wait();
        if (!(__res._M_error == nullptr)) rethrow_exception(__res._M_error);
        return static_cast<__result_type>(__res);
    }

    void _M_swap(__basic_future& __that) noexcept { _M_state.swap(__that._M_state); }

    // Construction of a future by promise::get_future()
    explicit __basic_future(const __state_type& __state) : _M_state(__state) {
        _State_base::_S_check(_M_state);
        _M_state->_M_set_retrieved_flag();
    }

    // Copy construction from a shared_future
    explicit __basic_future(const shared_future<_Res>&) noexcept;

    // Move construction from a shared_future
    explicit __basic_future(shared_future<_Res>&&) noexcept;

    // Move construction from a future
    explicit __basic_future(future<_Res>&&) noexcept;

    constexpr __basic_future() noexcept : _M_state() {}

    struct _Reset {
        explicit _Reset(__basic_future& __fut) noexcept : _M_fut(__fut) {}
        ~_Reset() { _M_fut._M_state.reset(); }
        __basic_future& _M_fut;
    };
};

/// Primary template for future.
template <typename _Res>
class future : public __basic_future<_Res> {
    // _GLIBCXX_RESOLVE_LIB_DEFECTS
    // 3458. Is shared_future intended to work with arrays or function types?
    static_assert(!std::is_array_v<_Res>, "result type must not be an array");
    static_assert(!std::is_function_v<_Res>, "result type must not be a function");
    static_assert(std::is_destructible_v<_Res>, "result type must be destructible");

    friend class promise<_Res>;
    template <typename>
    friend class packaged_task;

    typedef __basic_future<_Res> _Base_type;
    typedef typename _Base_type::__state_type __state_type;

    explicit future(const __state_type& __state) : _Base_type(__state) {}

public:
    constexpr future() noexcept : _Base_type() {}

    /// Move constructor
    future(future&& __uf) noexcept : _Base_type(std::move(__uf)) {}

    // Disable copying
    future(const future&) = delete;
    future& operator=(const future&) = delete;

    future& operator=(future&& __fut) noexcept {
        future(std::move(__fut))._M_swap(*this);
        return *this;
    }

    /// Retrieving the value
    _Res get() {
        typename _Base_type::_Reset __reset(*this);
        return std::move(this->_M_get_result()._M_value());
    }

    shared_future<_Res> share() noexcept;
};

/// Partial specialization for future<R&>
template <typename _Res>
class future<_Res&> : public __basic_future<_Res&> {
    friend class promise<_Res&>;
    template <typename>
    friend class packaged_task;

    typedef __basic_future<_Res&> _Base_type;
    typedef typename _Base_type::__state_type __state_type;

    explicit future(const __state_type& __state) : _Base_type(__state) {}

public:
    constexpr future() noexcept : _Base_type() {}

    /// Move constructor
    future(future&& __uf) noexcept : _Base_type(std::move(__uf)) {}

    // Disable copying
    future(const future&) = delete;
    future& operator=(const future&) = delete;

    future& operator=(future&& __fut) noexcept {
        future(std::move(__fut))._M_swap(*this);
        return *this;
    }

    /// Retrieving the value
    _Res& get() {
        typename _Base_type::_Reset __reset(*this);
        return this->_M_get_result()._M_get();
    }

    shared_future<_Res&> share() noexcept;
};

/// Explicit specialization for future<void>
template <>
class future<void> : public __basic_future<void> {
    friend class promise<void>;
    template <typename>
    friend class packaged_task;

    typedef __basic_future<void> _Base_type;
    typedef typename _Base_type::__state_type __state_type;

    explicit future(const __state_type& __state) : _Base_type(__state) {}

public:
    constexpr future() noexcept : _Base_type() {}

    /// Move constructor
    future(future&& __uf) noexcept : _Base_type(std::move(__uf)) {}

    // Disable copying
    future(const future&) = delete;
    future& operator=(const future&) = delete;

    future& operator=(future&& __fut) noexcept {
        future(std::move(__fut))._M_swap(*this);
        return *this;
    }

    /// Retrieving the value
    void get() {
        typename _Base_type::_Reset __reset(*this);
        this->_M_get_result();
    }

    shared_future<void> share() noexcept;
};

/// Primary template for shared_future.
template <typename _Res>
class shared_future : public __basic_future<_Res> {
    // _GLIBCXX_RESOLVE_LIB_DEFECTS
    // 3458. Is shared_future intended to work with arrays or function types?
    static_assert(!std::is_array_v<_Res>, "result type must not be an array");
    static_assert(!std::is_function_v<_Res>, "result type must not be a function");
    static_assert(std::is_destructible_v<_Res>, "result type must be destructible");

    typedef __basic_future<_Res> _Base_type;

public:
    constexpr shared_future() noexcept : _Base_type() {}

    /// Copy constructor
    shared_future(const shared_future& __sf) noexcept : _Base_type(__sf) {}

    /// Construct from a future rvalue
    shared_future(future<_Res>&& __uf) noexcept : _Base_type(std::move(__uf)) {}

    /// Construct from a shared_future rvalue
    shared_future(shared_future&& __sf) noexcept : _Base_type(std::move(__sf)) {}

    shared_future& operator=(const shared_future& __sf) noexcept {
        shared_future(__sf)._M_swap(*this);
        return *this;
    }

    shared_future& operator=(shared_future&& __sf) noexcept {
        shared_future(std::move(__sf))._M_swap(*this);
        return *this;
    }

    /// Retrieving the value
    const _Res& get() const { return this->_M_get_result()._M_value(); }
};

/// Partial specialization for shared_future<R&>
template <typename _Res>
class shared_future<_Res&> : public __basic_future<_Res&> {
    typedef __basic_future<_Res&> _Base_type;

public:
    constexpr shared_future() noexcept : _Base_type() {}

    /// Copy constructor
    shared_future(const shared_future& __sf) : _Base_type(__sf) {}

    /// Construct from a future rvalue
    shared_future(future<_Res&>&& __uf) noexcept : _Base_type(std::move(__uf)) {}

    /// Construct from a shared_future rvalue
    shared_future(shared_future&& __sf) noexcept : _Base_type(std::move(__sf)) {}

    shared_future& operator=(const shared_future& __sf) {
        shared_future(__sf)._M_swap(*this);
        return *this;
    }

    shared_future& operator=(shared_future&& __sf) noexcept {
        shared_future(std::move(__sf))._M_swap(*this);
        return *this;
    }

    /// Retrieving the value
    _Res& get() const { return this->_M_get_result()._M_get(); }
};

/// Explicit specialization for shared_future<void>
template <>
class shared_future<void> : public __basic_future<void> {
    typedef __basic_future<void> _Base_type;

public:
    constexpr shared_future() noexcept : _Base_type() {}

    /// Copy constructor
    shared_future(const shared_future& __sf) : _Base_type(__sf) {}

    /// Construct from a future rvalue
    shared_future(future<void>&& __uf) noexcept : _Base_type(std::move(__uf)) {}

    /// Construct from a shared_future rvalue
    shared_future(shared_future&& __sf) noexcept : _Base_type(std::move(__sf)) {}

    shared_future& operator=(const shared_future& __sf) {
        shared_future(__sf)._M_swap(*this);
        return *this;
    }

    shared_future& operator=(shared_future&& __sf) noexcept {
        shared_future(std::move(__sf))._M_swap(*this);
        return *this;
    }

    // Retrieving the value
    void get() const { this->_M_get_result(); }
};

// Now we can define the protected __basic_future constructors.
template <typename _Res>
inline __basic_future<_Res>::__basic_future(const shared_future<_Res>& __sf) noexcept : _M_state(__sf._M_state) {}

template <typename _Res>
inline __basic_future<_Res>::__basic_future(shared_future<_Res>&& __sf) noexcept : _M_state(std::move(__sf._M_state)) {}

template <typename _Res>
inline __basic_future<_Res>::__basic_future(future<_Res>&& __uf) noexcept : _M_state(std::move(__uf._M_state)) {}

// _GLIBCXX_RESOLVE_LIB_DEFECTS
// 2556. Wide contract for future::share()
template <typename _Res>
inline shared_future<_Res> future<_Res>::share() noexcept {
    return shared_future<_Res>(std::move(*this));
}

template <typename _Res>
inline shared_future<_Res&> future<_Res&>::share() noexcept {
    return shared_future<_Res&>(std::move(*this));
}

inline shared_future<void> future<void>::share() noexcept {
    return shared_future<void>(std::move(*this));
}

/// Primary template for promise
template <typename _Res>
class promise {
    // _GLIBCXX_RESOLVE_LIB_DEFECTS
    // 3466: Specify the requirements for promise/future/[...] consistently
    static_assert(!std::is_array_v<_Res>, "result type must not be an array");
    static_assert(!std::is_function_v<_Res>, "result type must not be a function");
    static_assert(std::is_destructible_v<_Res>, "result type must be destructible");

    typedef __future_base::_State_base _State;
    typedef __future_base::_Result<_Res> _Res_type;
    typedef __future_base::_Ptr<_Res_type> _Ptr_type;
    template <typename, typename>
    friend struct _State::_Setter;
    friend _State;

    std::shared_ptr<_State> _M_future;
    _Ptr_type _M_storage;

public:
    promise() : _M_future(std::make_shared<_State>()), _M_storage(new _Res_type()) {}

    promise(promise&& __rhs) noexcept
            : _M_future(std::move(__rhs._M_future)), _M_storage(std::move(__rhs._M_storage)) {}

    promise(const promise&) = delete;

    ~promise() {
        if (static_cast<bool>(_M_future) && !_M_future.unique()) _M_future->_M_break_promise(std::move(_M_storage));
    }

    // Assignment
    promise& operator=(promise&& __rhs) noexcept {
        promise(std::move(__rhs)).swap(*this);
        return *this;
    }

    promise& operator=(const promise&) = delete;

    void swap(promise& __rhs) noexcept {
        _M_future.swap(__rhs._M_future);
        _M_storage.swap(__rhs._M_storage);
    }

    // Retrieving the result
    future<_Res> get_future() { return future<_Res>(_M_future); }

    // Setting the result
    void set_value(const _Res& __r) { _M_state()._M_set_result(_State::__setter(this, __r)); }

    void set_value(_Res&& __r) { _M_state()._M_set_result(_State::__setter(this, std::move(__r))); }

    void set_exception(std::exception_ptr __p) { _M_state()._M_set_result(_State::__setter(__p, this)); }

    void set_value_at_pthread_exit(const _Res& __r) {
        _M_state()._M_set_delayed_result(_State::__setter(this, __r), _M_future);
    }

    void set_value_at_pthread_exit(_Res&& __r) {
        _M_state()._M_set_delayed_result(_State::__setter(this, std::move(__r)), _M_future);
    }

    void set_exception_at_pthread_exit(std::exception_ptr __p) {
        _M_state()._M_set_delayed_result(_State::__setter(__p, this), _M_future);
    }

private:
    _State& _M_state() {
        __future_base::_State_base::_S_check(_M_future);
        return *_M_future;
    }
};

template <typename _Res>
inline void swap(promise<_Res>& __x, promise<_Res>& __y) noexcept {
    __x.swap(__y);
}

/// Partial specialization for promise<R&>
template <typename _Res>
class promise<_Res&> {
    typedef __future_base::_State_base _State;
    typedef __future_base::_Result<_Res&> _Res_type;
    typedef __future_base::_Ptr<_Res_type> _Ptr_type;
    template <typename, typename>
    friend struct _State::_Setter;
    friend _State;

    std::shared_ptr<_State> _M_future;
    _Ptr_type _M_storage;

public:
    promise() : _M_future(std::make_shared<_State>()), _M_storage(new _Res_type()) {}

    promise(promise&& __rhs) noexcept
            : _M_future(std::move(__rhs._M_future)), _M_storage(std::move(__rhs._M_storage)) {}

    promise(const promise&) = delete;

    ~promise() {
        if (static_cast<bool>(_M_future) && !_M_future.unique()) _M_future->_M_break_promise(std::move(_M_storage));
    }

    // Assignment
    promise& operator=(promise&& __rhs) noexcept {
        promise(std::move(__rhs)).swap(*this);
        return *this;
    }

    promise& operator=(const promise&) = delete;

    void swap(promise& __rhs) noexcept {
        _M_future.swap(__rhs._M_future);
        _M_storage.swap(__rhs._M_storage);
    }

    // Retrieving the result
    future<_Res&> get_future() { return future<_Res&>(_M_future); }

    // Setting the result
    void set_value(_Res& __r) { _M_state()._M_set_result(_State::__setter(this, __r)); }

    void set_exception(std::exception_ptr __p) { _M_state()._M_set_result(_State::__setter(__p, this)); }

    void set_value_at_pthread_exit(_Res& __r) {
        _M_state()._M_set_delayed_result(_State::__setter(this, __r), _M_future);
    }

    void set_exception_at_pthread_exit(std::exception_ptr __p) {
        _M_state()._M_set_delayed_result(_State::__setter(__p, this), _M_future);
    }

private:
    _State& _M_state() {
        __future_base::_State_base::_S_check(_M_future);
        return *_M_future;
    }
};

/// Explicit specialization for promise<void>
template <>
class promise<void> {
    typedef __future_base::_State_base _State;
    typedef __future_base::_Result<void> _Res_type;
    typedef __future_base::_Ptr<_Res_type> _Ptr_type;
    template <typename, typename>
    friend struct _State::_Setter;
    friend _State;

    std::shared_ptr<_State> _M_future;
    _Ptr_type _M_storage;

public:
    promise() : _M_future(std::make_shared<_State>()), _M_storage(new _Res_type()) {}

    promise(promise&& __rhs) noexcept
            : _M_future(std::move(__rhs._M_future)), _M_storage(std::move(__rhs._M_storage)) {}

    promise(const promise&) = delete;

    ~promise() {
        if (static_cast<bool>(_M_future) && !_M_future.unique()) _M_future->_M_break_promise(std::move(_M_storage));
    }

    // Assignment
    promise& operator=(promise&& __rhs) noexcept {
        promise(std::move(__rhs)).swap(*this);
        return *this;
    }

    promise& operator=(const promise&) = delete;

    void swap(promise& __rhs) noexcept {
        _M_future.swap(__rhs._M_future);
        _M_storage.swap(__rhs._M_storage);
    }

    // Retrieving the result
    future<void> get_future() { return future<void>(_M_future); }

    // Setting the result
    void set_value() { _M_state()._M_set_result(_State::__setter(this)); }

    void set_exception(std::exception_ptr __p) { _M_state()._M_set_result(_State::__setter(__p, this)); }

    void set_value_at_pthread_exit() { _M_state()._M_set_delayed_result(_State::__setter(this), _M_future); }

    void set_exception_at_pthread_exit(std::exception_ptr __p) {
        _M_state()._M_set_delayed_result(_State::__setter(__p, this), _M_future);
    }

private:
    _State& _M_state() {
        __future_base::_State_base::_S_check(_M_future);
        return *_M_future;
    }
};

template <typename _Ptr_type, typename _Fn, typename _Res>
struct __future_base::_Task_setter {
    // Invoke the function and provide the result to the caller.
    _Ptr_type operator()() const {
        try {
            (*_M_result)->_M_set((*_M_fn)());
        } catch (...) {
            (*_M_result)->_M_error = std::current_exception();
        }
        return std::move(*_M_result);
    }
    _Ptr_type* _M_result;
    _Fn* _M_fn;
};

template <typename _Ptr_type, typename _Fn>
struct __future_base::_Task_setter<_Ptr_type, _Fn, void> {
    _Ptr_type operator()() const {
        try {
            (*_M_fn)();
        } catch (...) {
            (*_M_result)->_M_error = std::current_exception();
        }
        return std::move(*_M_result);
    }
    _Ptr_type* _M_result;
    _Fn* _M_fn;
};

// Holds storage for a packaged_task's result.
template <typename _Res, typename... _Args>
struct __future_base::_Task_state_base<_Res(_Args...)> : __future_base::_State_base {
    typedef _Res _Res_type;

    template <typename _Alloc>
    _Task_state_base(const _Alloc& __a) : _M_result(_S_allocate_result<_Res>(__a)) {}

    // Invoke the stored task and make the state ready.
    virtual void _M_run(_Args&&... __args) = 0;

    // Invoke the stored task and make the state ready at thread exit.
    virtual void _M_run_delayed(_Args&&... __args, std::weak_ptr<_State_base>) = 0;

    virtual std::shared_ptr<_Task_state_base> _M_reset() = 0;

    typedef __future_base::_Ptr<_Result<_Res>> _Ptr_type;
    _Ptr_type _M_result;
};

// Holds a packaged_task's stored task.
template <typename _Fn, typename _Alloc, typename _Res, typename... _Args>
struct __future_base::_Task_state<_Fn, _Alloc, _Res(_Args...)> final : __future_base::_Task_state_base<_Res(_Args...)> {
    template <typename _Fn2>
    _Task_state(_Fn2&& __fn, const _Alloc& __a)
            : _Task_state_base<_Res(_Args...)>(__a), _M_impl(std::forward<_Fn2>(__fn), __a) {}

private:
    virtual void _M_run(_Args&&... __args) {
        auto __boundfn = [&]() -> _Res { return std::__invoke_r<_Res>(_M_impl._M_fn, std::forward<_Args>(__args)...); };
        this->_M_set_result(_S_task_setter(this->_M_result, __boundfn));
    }

    virtual void _M_run_delayed(_Args&&... __args, std::weak_ptr<_State_base> __self) {
        auto __boundfn = [&]() -> _Res { return std::__invoke_r<_Res>(_M_impl._M_fn, std::forward<_Args>(__args)...); };
        this->_M_set_delayed_result(_S_task_setter(this->_M_result, __boundfn), std::move(__self));
    }

    virtual std::shared_ptr<_Task_state_base<_Res(_Args...)>> _M_reset();

    struct _Impl : _Alloc {
        template <typename _Fn2>
        _Impl(_Fn2&& __fn, const _Alloc& __a) : _Alloc(__a), _M_fn(std::forward<_Fn2>(__fn)) {}
        _Fn _M_fn;
    } _M_impl;
};

template <typename _Signature, typename _Fn, typename _Alloc = std::allocator<int>>
static std::shared_ptr<__future_base::_Task_state_base<_Signature>> __my_create_task_state(
        _Fn&& __fn, const _Alloc& __a = _Alloc()) {
    typedef typename std::decay<_Fn>::type _Fn2;
    typedef __future_base::_Task_state<_Fn2, _Alloc, _Signature> _State;
    return std::allocate_shared<_State>(__a, std::forward<_Fn>(__fn), __a);
}

template <typename _Fn, typename _Alloc, typename _Res, typename... _Args>
std::shared_ptr<__future_base::_Task_state_base<_Res(_Args...)>>
__future_base::_Task_state<_Fn, _Alloc, _Res(_Args...)>::_M_reset() {
    return __my_create_task_state<_Res(_Args...)>(std::move(_M_impl._M_fn), static_cast<_Alloc&>(_M_impl));
}

/// packaged_task
template <typename _Res, typename... _ArgTypes>
class packaged_task<_Res(_ArgTypes...)> {
    typedef __future_base::_Task_state_base<_Res(_ArgTypes...)> _State_type;
    std::shared_ptr<_State_type> _M_state;

    // _GLIBCXX_RESOLVE_LIB_DEFECTS
    // 3039. Unnecessary decay in thread and packaged_task
    template <typename _Fn, typename _Fn2 = std::remove_cvref_t<_Fn>>
    using __not_same = typename std::enable_if<!std::is_same<packaged_task, _Fn2>::value>::type;

public:
    // Construction and destruction
    packaged_task() noexcept {}

    template <typename _Fn, typename = __not_same<_Fn>>
    explicit packaged_task(_Fn&& __fn)
            : _M_state(__my_create_task_state<_Res(_ArgTypes...)>(std::forward<_Fn>(__fn))) {}

    ~packaged_task() {
        if (static_cast<bool>(_M_state) && !_M_state.unique())
            _M_state->_M_break_promise(std::move(_M_state->_M_result));
    }

    // No copy
    packaged_task(const packaged_task&) = delete;
    packaged_task& operator=(const packaged_task&) = delete;

    // Move support
    packaged_task(packaged_task&& __other) noexcept { this->swap(__other); }

    packaged_task& operator=(packaged_task&& __other) noexcept {
        packaged_task(std::move(__other)).swap(*this);
        return *this;
    }

    void swap(packaged_task& __other) noexcept { _M_state.swap(__other._M_state); }

    bool valid() const noexcept { return static_cast<bool>(_M_state); }

    // Result retrieval
    future<_Res> get_future() { return future<_Res>(_M_state); }

    // Execution
    void operator()(_ArgTypes... __args) {
        __future_base::_State_base::_S_check(_M_state);
        _M_state->_M_run(std::forward<_ArgTypes>(__args)...);
    }

    void make_ready_at_pthread_exit(_ArgTypes... __args) {
        __future_base::_State_base::_S_check(_M_state);
        _M_state->_M_run_delayed(std::forward<_ArgTypes>(__args)..., _M_state);
    }

    void reset() {
        __future_base::_State_base::_S_check(_M_state);
        packaged_task __tmp;
        __tmp._M_state = _M_state;
        _M_state = _M_state->_M_reset();
    }
};

template <typename>
struct __function_guide_helper {};

template <typename _Res, typename _Tp, bool _Nx, typename... _Args>
struct __function_guide_helper<_Res (_Tp::*)(_Args...) noexcept(_Nx)> {
    using type = _Res(_Args...);
};

template <typename _Res, typename _Tp, bool _Nx, typename... _Args>
struct __function_guide_helper<_Res (_Tp::*)(_Args...)& noexcept(_Nx)> {
    using type = _Res(_Args...);
};

template <typename _Res, typename _Tp, bool _Nx, typename... _Args>
struct __function_guide_helper<_Res (_Tp::*)(_Args...) const noexcept(_Nx)> {
    using type = _Res(_Args...);
};

template <typename _Res, typename _Tp, bool _Nx, typename... _Args>
struct __function_guide_helper<_Res (_Tp::*)(_Args...) const& noexcept(_Nx)> {
    using type = _Res(_Args...);
};

//template<typename _Res, typename... _ArgTypes>
//function(_Res(*)(_ArgTypes...)) -> function<_Res(_ArgTypes...)>;
//
//template<typename _Functor, typename _Signature = typename
//                             __function_guide_helper<decltype(&_Functor::operator())>::type>
//function(_Functor) -> function<_Signature>;

template <typename _Res, typename... _ArgTypes>
packaged_task(_Res (*)(_ArgTypes...)) -> packaged_task<_Res(_ArgTypes...)>;

template <typename _Fun, typename _Signature = typename __function_guide_helper<decltype(&_Fun::operator())>::type>
packaged_task(_Fun) -> packaged_task<_Signature>;

/// swap
template <typename _Res, typename... _ArgTypes>
inline void swap(packaged_task<_Res(_ArgTypes...)>& __x, packaged_task<_Res(_ArgTypes...)>& __y) noexcept {
    __x.swap(__y);
}
} // namespace starrocks::bthreads