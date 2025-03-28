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

#include <bthread/bthread.h>
#include <glog/logging.h>
#include <pthread.h>

#include <optional>
#include <shared_mutex>
#include <thread>
#include <type_traits>

namespace starrocks {

enum AssertHeldState {
    UNLOCKED = 0,
    SHARED_LOCK = 1,
    EXCLUSIVE_LOCK = 2,
};

template <typename T, typename = std::enable_if_t<std::is_copy_assignable<T>::value, T>>
class BTlsObject {
public:
    explicit BTlsObject() { _create_succ = (bthread_key_create(&_key, delete_handler) == 0); }
    ~BTlsObject() {
        if (_create_succ) {
            void* old = bthread_getspecific(_key);
            if (old != nullptr) {
                delete_handler(old);
                bthread_setspecific(_key, nullptr);
            }
            bthread_key_delete(_key);
        }
    }
    template <typename... Args>
    void set(Args&&... args) {
        if (_create_succ) {
            void* old = bthread_getspecific(_key);
            if (old != nullptr) {
                ((T*)old)->reset(std::forward<Args>(args)...);
            } else {
                auto obj = new T(std::forward<Args>(args)...);
                int ret = bthread_setspecific(_key, obj);
                if (ret != 0) {
                    delete obj;
                }
            }
        }
    }

    std::optional<T> get() const {
        if (_create_succ) {
            void* old = bthread_getspecific(_key);
            if (old == nullptr) {
                return {};
            }
            return *(T*)old;
        } else {
            return {};
        }
    }

    void update(std::function<void(T*)> update_func) {
        if (_create_succ) {
            void* old = bthread_getspecific(_key);
            if (old != nullptr) {
                update_func((T*)old);
            }
        }
    }

    bool create_succ() const { return _create_succ; }

    static void delete_handler(void* p) { delete (T*)p; }

private:
    bthread_key_t _key;
    // may create fail because limit, skip check
    bool _create_succ;
};

struct AssertHeldInfo {
    explicit AssertHeldInfo(AssertHeldState s) { reset(s); }
    void reset(AssertHeldState s) {
        state = s;
        shared_lock_cnt = (state == AssertHeldState::SHARED_LOCK) ? 1 : 0;
        owner = pthread_self();
    }
    AssertHeldState state;
    uint32_t shared_lock_cnt;
    // can be used to detect deadlock
    pthread_t owner;
};

class AssertHeldSharedMutex : public std::shared_mutex {
public:
    void lock() {
        std::shared_mutex::lock();
        _held_info.set(AssertHeldState::EXCLUSIVE_LOCK);
    }
    bool try_lock() {
        if (std::shared_mutex::try_lock()) {
            _held_info.set(AssertHeldState::EXCLUSIVE_LOCK);
            return true;
        }
        return false;
    }
    void unlock() {
        std::shared_mutex::unlock();
        _held_info.set(AssertHeldState::UNLOCKED);
    }

    // Shared ownership

    void lock_shared() {
        std::shared_mutex::lock_shared();
        _inc_shared_lock();
    }

    bool try_lock_shared() {
        if (std::shared_mutex::try_lock_shared()) {
            _inc_shared_lock();
            return true;
        }
        return false;
    }

    void unlock_shared() {
        std::shared_mutex::unlock_shared();
        _dec_shared_lock();
    }
    bool assert_held_shared() {
        if (!_held_info.create_succ()) {
            // assume check success when create thread local var fail.
            return true;
        }
        auto info = _held_info.get();
        return info.has_value() && info.value().state == AssertHeldState::SHARED_LOCK;
    }
    bool assert_held_exclusive() {
        if (!_held_info.create_succ()) {
            // assume check success when create thread local var fail.
            return true;
        }
        auto info = _held_info.get();
        return info.has_value() && info.value().state == AssertHeldState::EXCLUSIVE_LOCK;
    }

    int TEST_get_shared_lock_cnt() {
        if (!_held_info.create_succ()) {
            return 0;
        }
        auto info = _held_info.get();
        if (!info.has_value() || info.value().state != AssertHeldState::SHARED_LOCK) {
            return 0;
        }
        return info.value().shared_lock_cnt;
    }

private:
    // for reentrance
    void _inc_shared_lock() {
        auto info = _held_info.get();
        if (info.has_value()) {
            _held_info.update([](AssertHeldInfo* info) {
                info->state = AssertHeldState::SHARED_LOCK;
                info->shared_lock_cnt++;
            });
        } else {
            _held_info.set(AssertHeldState::SHARED_LOCK);
        }
    }

    void _dec_shared_lock() {
        auto info = _held_info.get();
        if (info.has_value()) {
            _held_info.update([](AssertHeldInfo* info) {
                info->shared_lock_cnt--;
                if (info->shared_lock_cnt == 0) {
                    info->state = AssertHeldState::UNLOCKED;
                }
            });
        } else {
            _held_info.set(AssertHeldState::UNLOCKED);
        }
    }

    BTlsObject<AssertHeldInfo> _held_info;
};

class AssertHeldMutex : public std::mutex {
public:
    void lock() {
        std::mutex::lock();
        _held_info.set(AssertHeldState::EXCLUSIVE_LOCK);
    }
    bool try_lock() {
        if (std::mutex::try_lock()) {
            _held_info.set(AssertHeldState::EXCLUSIVE_LOCK);
            return true;
        }
        return false;
    }
    void unlock() {
        std::mutex::unlock();
        _held_info.set(AssertHeldState::UNLOCKED);
    }
    bool assert_held() {
        if (!_held_info.create_succ()) {
            // assume check success when create thread local var fail.
            return true;
        }
        auto info = _held_info.get();
        return info.has_value() && info.value().state == AssertHeldState::EXCLUSIVE_LOCK;
    }

private:
    BTlsObject<AssertHeldInfo> _held_info;
};

#if !defined(BE_TEST) && !defined(NDEBUG)
#define CHECK_MUTEX_HELD(mutex) DCHECK(mutex.assert_held() == true) << "mutex not held!";
#define CHECK_MUTEX_HELD_SHARED(mutex) \
    DCHECK(mutex.assert_held_shared() == true || mutex.assert_held_exclusive() == true) << "mutex not held!";
#define CHECK_MUTEX_HELD_EXCLUSIVE(mutex) DCHECK(mutex.assert_held_exclusive() == true) << "mutex not held!";

#else
#define CHECK_MUTEX_HELD(mutex) \
    {}
#define CHECK_MUTEX_HELD_SHARED(mutex) \
    {}
#define CHECK_MUTEX_HELD_EXCLUSIVE(mutex) \
    {}
#endif

} // namespace starrocks