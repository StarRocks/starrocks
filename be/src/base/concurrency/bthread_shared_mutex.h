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

#include <cassert>
#include <climits>
#include <mutex>

#include "bthread/condition_variable.h"
#include "bthread/mutex.h"

namespace starrocks::bthreads {

// Implement bthread specific shared mutex with bthread::Mutex and bthread::ConditionVariable
class BThreadSharedMutex {
private:
    typedef bthread::Mutex _mutex_type;
    typedef bthread::ConditionVariable _cv_type;

public:
    BThreadSharedMutex() : _state(0) {}
    ~BThreadSharedMutex() { assert(_state == 0); }

    BThreadSharedMutex(const BThreadSharedMutex&) = delete;
    BThreadSharedMutex& operator=(const BThreadSharedMutex&) = delete;

    void lock() {
        std::unique_lock<_mutex_type> lk(_mutex);
        while (has_writer()) {
            _gate1_cv.wait(lk);
        }
        _state |= _write_entered;
        while (num_readers() > 0) {
            _gate2_cv.wait(lk);
        }
    }

    bool try_lock() {
        std::unique_lock<_mutex_type> lk(_mutex, std::try_to_lock);
        if (lk.owns_lock() && _state == 0) {
            _state = _write_entered;
            return true;
        }
        return false;
    }

    void unlock() {
        std::lock_guard<_mutex_type> lk(_mutex);
        assert(has_writer());
        _state = 0;
        _gate1_cv.notify_all();
    }

    void lock_shared() {
        std::unique_lock<_mutex_type> lk(_mutex);
        while (_state >= _n_readers) {
            _gate1_cv.wait(lk);
        }
        ++_state;
    }

    bool try_lock_shared() {
        std::unique_lock<_mutex_type> lk(_mutex, std::try_to_lock);
        if (!lk.owns_lock()) {
            return false;
        }
        if (_state < _n_readers) {
            ++_state;
            return true;
        }
        return false;
    }

    void unlock_shared() {
        std::lock_guard<_mutex_type> lk(_mutex);
        assert(num_readers() > 0);
        auto prev = _state--;
        if (has_writer()) {
            if (num_readers() == 0) {
                _gate2_cv.notify_one();
            }
        } else {
            if (prev == _n_readers) {
                _gate1_cv.notify_one();
            }
        }
    }

private:
    inline bool has_writer() const { return _state & _write_entered; }
    inline unsigned num_readers() const { return _state & _n_readers; }

private:
    // highest bit as the writer flag, only one writer allowed
    static constexpr unsigned _write_entered = 1U << (sizeof(unsigned) * CHAR_BIT - 1);
    // remain bits as the reader counter
    static constexpr unsigned _n_readers = ~_write_entered;

    _mutex_type _mutex;
    _cv_type _gate1_cv;
    _cv_type _gate2_cv;
    unsigned _state;
};

} // namespace starrocks::bthreads
