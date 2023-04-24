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
// <shared_mutex> -*- C++ -*-

// Copyright (C) 2013-2023 Free Software Foundation, Inc.
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

// original source location refer to:
//  https://github.com/gcc-mirror/gcc/blob/master/libstdc%2B%2B-v3/include/std/shared_mutex

#pragma once

#include <cassert>
#include <climits>
#include <mutex>

#include "bthread/condition_variable.h"
#include "bthread/mutex.h"

namespace starrocks::bthreads {

/// A shared mutex type implemented using condition_variable.
template <typename M, typename CV>
class __shared_mutex_cv {
public:
    typedef M _mutex_type;
    typedef CV _condition_variable_type;

private:
    // Based on Howard Hinnant's reference implementation from N2406.

    // The high bit of _M_state is the write-entered flag which is set to
    // indicate a writer has taken the lock or is queuing to take the lock.
    // The remaining bits are the count of reader locks.
    //
    // To take a reader lock, block on gate1 while the write-entered flag is
    // set or the maximum number of reader locks is held, then increment the
    // reader lock count.
    // To release, decrement the count, then if the write-entered flag is set
    // and the count is zero then signal gate2 to wake a queued writer,
    // otherwise if the maximum number of reader locks was held signal gate1
    // to wake a reader.
    //
    // To take a writer lock, block on gate1 while the write-entered flag is
    // set, then set the write-entered flag to start queueing, then block on
    // gate2 while the number of reader locks is non-zero.
    // To release, unset the write-entered flag and signal gate1 to wake all
    // blocked readers and writers.
    //
    // This means that when no reader locks are held readers and writers get
    // equal priority. When one or more reader locks is held a writer gets
    // priority and no more reader locks can be taken while the writer is
    // queued.

    // Only locked when accessing _M_state or waiting on condition variables.
    _mutex_type _M_mut;
    // Used to block while write-entered is set or reader count at maximum.
    _condition_variable_type _M_gate1;
    // Used to block queued writers while reader count is non-zero.
    _condition_variable_type _M_gate2;
    // The write-entered flag and reader count.
    unsigned _M_state;

    static constexpr unsigned _S_write_entered = 1U << (sizeof(unsigned) * CHAR_BIT - 1);
    static constexpr unsigned _S_max_readers = ~_S_write_entered;

    // Test whether the write-entered flag is set. _M_mut must be locked.
    bool _M_write_entered() const { return _M_state & _S_write_entered; }

    // The number of reader locks currently held. _M_mut must be locked.
    unsigned _M_readers() const { return _M_state & _S_max_readers; }

public:
    __shared_mutex_cv() : _M_state(0) {}

    ~__shared_mutex_cv() { assert(_M_state == 0); }

    __shared_mutex_cv(const __shared_mutex_cv&) = delete;
    __shared_mutex_cv& operator=(const __shared_mutex_cv&) = delete;

    // Exclusive ownership
    void lock() {
        std::unique_lock<_mutex_type> __lk(_M_mut);
        // Wait until we can set the write-entered flag.
        // _M_gate1.wait(__lk, [=] { return !_M_write_entered(); });
        wait(_M_gate1, __lk, [=] { return !_M_write_entered(); });
        _M_state |= _S_write_entered;
        // Then wait until there are no more readers.
        // _M_gate2.wait(__lk, [=] { return _M_readers() == 0; });
        wait(_M_gate2, __lk, [=] { return _M_readers() == 0; });
    }

    bool try_lock() {
        std::unique_lock<_mutex_type> __lk(_M_mut, std::try_to_lock);
        if (__lk.owns_lock() && _M_state == 0) {
            _M_state = _S_write_entered;
            return true;
        }
        return false;
    }

    void unlock() {
        std::lock_guard<_mutex_type> __lk(_M_mut);
        assert(_M_write_entered());
        _M_state = 0;
        // call notify_all() while mutex is held so that another thread can't
        // lock and unlock the mutex then destroy *this before we make the call.
        _M_gate1.notify_all();
    }

    // Shared ownership
    void lock_shared() {
        std::unique_lock<_mutex_type> __lk(_M_mut);
        // _M_gate1.wait(__lk, [=] { return _M_state < _S_max_readers; });
        wait(_M_gate1, __lk, [=] { return _M_state < _S_max_readers; });
        ++_M_state;
    }

    bool try_lock_shared() {
        std::unique_lock<_mutex_type> __lk(_M_mut, std::try_to_lock);
        if (!__lk.owns_lock()) return false;
        if (_M_state < _S_max_readers) {
            ++_M_state;
            return true;
        }
        return false;
    }

    void unlock_shared() {
        std::lock_guard<_mutex_type> __lk(_M_mut);
        assert(_M_readers() > 0);
        auto __prev = _M_state--;
        if (_M_write_entered()) {
            // Wake the queued writer if there are no more readers.
            if (_M_readers() == 0) _M_gate2.notify_one();
            // No need to notify gate1 because we give priority to the queued
            // writer, and that writer will eventually notify gate1 after it
            // clears the write-entered flag.
        } else {
            // Wake any thread that was blocked on reader overflow.
            if (__prev == _S_max_readers) _M_gate1.notify_one();
        }
    }

private:
    template <class Predicate>
    void wait(_condition_variable_type& cond, std::unique_lock<_mutex_type>& lock, Predicate stop_waiting) {
        while (!stop_waiting()) {
            cond.wait(lock);
        }
    }
};

typedef __shared_mutex_cv<bthread::Mutex, bthread::ConditionVariable> BThreadSharedMutex;

} // namespace starrocks::bthreads
