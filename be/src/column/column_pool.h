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

#include <butil/thread_local.h>
#include <butil/time.h> // NOLINT
#include <bvar/bvar.h>

#include <atomic>

#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/object_column.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "common/type_list.h"
#include "gutil/dynamic_annotations.h"
#include "runtime/current_thread.h"

namespace starrocks {

template <typename T>
struct ColumnPoolBlockSize {
    static const size_t value = 256;
};

template <>
struct ColumnPoolBlockSize<BinaryColumn> {
    static const size_t value = 128;
};

template <typename T>
int64_t column_bytes(const T* col) {
    static_assert(std::is_base_of_v<Column, T>, "must_derived_of_Column");
    return col->memory_usage();
}

template <typename T>
void reset_column(T* col) {
    static_assert(std::is_base_of_v<Column, T>, "must_derived_of_Column");
    col->reset_column();
    DCHECK(col->size() == 0);
    DCHECK(col->delete_state() == DEL_NOT_SATISFIED);
}

// Returns the number of bytes freed to tcmalloc.
template <typename T>
size_t release_column_if_large(T* col, size_t limit) {
    auto old_usage = column_bytes(col);
    if (old_usage < limit) {
        return 0;
    }
    if constexpr (std::is_same_v<BinaryColumn, T>) {
        auto& bytes = col->get_bytes();
        BinaryColumn::Bytes tmp;
        tmp.swap(bytes);
    } else {
        typename T::Container tmp;
        tmp.swap(col->get_data());
    }
    auto new_usage = column_bytes(col);
    DCHECK_LT(new_usage, old_usage);
    return old_usage - new_usage;
}

template <typename T>
size_t column_reserved_size(const T* col) {
    if constexpr (std::is_same_v<BinaryColumn, T>) {
        const auto& offsets = col->get_offset();
        return offsets.capacity() > 0 ? offsets.capacity() - 1 : 0;
    } else {
        return col->get_data().capacity();
    }
}

template <typename T, size_t NITEM>
struct ColumnPoolFreeBlock {
    int64_t nfree;
    int64_t bytes;
    T* ptrs[NITEM];
};

struct ColumnPoolInfo {
    size_t local_cnt = 0;
    size_t central_free_items = 0;
    size_t central_free_bytes = 0;
};

template <typename T>
class CACHELINE_ALIGNED ColumnPool {
    static constexpr size_t kBlockSize = ColumnPoolBlockSize<T>::value;

    using FreeBlock = ColumnPoolFreeBlock<T, kBlockSize>;
    using DynamicFreeBlock = ColumnPoolFreeBlock<T, 0>;

    class CACHELINE_ALIGNED LocalPool {
        static_assert(std::is_base_of<Column, T>::value, "Must_be_derived_of_Column");

    public:
        explicit LocalPool(ColumnPool* pool) : _pool(pool) {
            _curr_free.nfree = 0;
            _curr_free.bytes = 0;
        }

        ~LocalPool() noexcept {
            if (_curr_free.nfree > 0 && !_pool->_push_free_block(_curr_free)) {
                for (size_t i = 0; i < _curr_free.nfree; i++) {
                    ASAN_UNPOISON_MEMORY_REGION(_curr_free.ptrs[i], sizeof(T));
                    delete _curr_free.ptrs[i];
                }
            }
            _pool->_clear_from_destructor_of_local_pool();
        }

        T* get_object() {
            if (_curr_free.nfree == 0) {
                if (!_pool->_pop_free_block(&_curr_free)) {
                    return nullptr;
                }
            }
            T* obj = _curr_free.ptrs[--_curr_free.nfree];
            ASAN_UNPOISON_MEMORY_REGION(obj, sizeof(T));
            auto bytes = column_bytes(obj);
            _curr_free.bytes -= bytes;

            tls_thread_status.mem_consume(bytes);
            _pool->mem_tracker()->release(bytes);

            return obj;
        }

        void return_object(T* ptr, size_t chunk_size) {
            if (UNLIKELY(column_reserved_size(ptr) > chunk_size)) {
                delete ptr;
                return;
            }
            auto bytes = column_bytes(ptr);
            if (_curr_free.nfree < kBlockSize) {
                ASAN_POISON_MEMORY_REGION(ptr, sizeof(T));
                _curr_free.ptrs[_curr_free.nfree++] = ptr;
                _curr_free.bytes += bytes;

                tls_thread_status.mem_release(bytes);
                _pool->mem_tracker()->consume(bytes);

                return;
            }
            if (_pool->_push_free_block(_curr_free)) {
                ASAN_POISON_MEMORY_REGION(ptr, sizeof(T));
                _curr_free.nfree = 1;
                _curr_free.ptrs[0] = ptr;
                _curr_free.bytes = bytes;

                tls_thread_status.mem_release(bytes);
                _pool->mem_tracker()->consume(bytes);

                return;
            }
            delete ptr;
        }

        void release_large_columns(size_t limit) {
            size_t freed_bytes = 0;
            for (size_t i = 0; i < _curr_free.nfree; i++) {
                ASAN_UNPOISON_MEMORY_REGION(_curr_free.ptrs[i], sizeof(T));
                freed_bytes += release_column_if_large(_curr_free.ptrs[i], limit);
                ASAN_POISON_MEMORY_REGION(_curr_free.ptrs[i], sizeof(T));
            }
            if (freed_bytes > 0) {
                _curr_free.bytes -= freed_bytes;
                tls_thread_status.mem_consume(freed_bytes);
                _pool->mem_tracker()->release(freed_bytes);
            }
        }

        static void delete_local_pool(void* arg) { delete (LocalPool*)arg; }

    private:
        ColumnPool* _pool;
        FreeBlock _curr_free;
    };

public:
    void set_mem_tracker(std::shared_ptr<MemTracker> mem_tracker) { _mem_tracker = std::move(mem_tracker); }
    MemTracker* mem_tracker() { return _mem_tracker.get(); }

    static std::enable_if_t<std::is_default_constructible_v<T>, ColumnPool*> singleton() {
        static ColumnPool p;
        return &p;
    }

    template <bool AllocOnEmpty = true>
    T* get_column() {
        LocalPool* lp = _get_or_new_local_pool();
        if (UNLIKELY(lp == nullptr)) {
            return nullptr;
        }
        T* ptr = lp->get_object();
        if (ptr == nullptr && AllocOnEmpty) {
            ptr = new (std::nothrow) T();
        }
        return ptr;
    }

    void return_column(T* ptr, size_t chunk_size) {
        LocalPool* lp = _get_or_new_local_pool();
        if (LIKELY(lp != nullptr)) {
            reset_column(ptr);
            lp->return_object(ptr, chunk_size);
        } else {
            delete ptr;
        }
    }

    // Destroy some objects in the *central* free list.
    // Returns the number of bytes freed to tcmalloc.
    size_t release_free_columns(double free_ratio) {
        free_ratio = std::min<double>(free_ratio, 1.0);
        int64_t now = butil::gettimeofday_s();
        std::vector<DynamicFreeBlock*> tmp;
        if (now - _first_push_time > 3) {
            //    ^^^^^^^^^^^^^^^^ read without lock by intention.
            std::lock_guard<std::mutex> l(_free_blocks_lock);
            int n = implicit_cast<int>(static_cast<base::identity_<int>::type>(
                    static_cast<double>(_free_blocks.size()) * (1 - free_ratio)));
            tmp.insert(tmp.end(), _free_blocks.begin() + n, _free_blocks.end());
            _free_blocks.resize(n);
        }
        size_t freed_bytes = 0;
        for (DynamicFreeBlock* blk : tmp) {
            freed_bytes += blk->bytes;
            _release_free_block(blk);
        }

        _mem_tracker->release(freed_bytes);
        tls_thread_status.mem_consume(freed_bytes);

        return freed_bytes;
    }

    // Reduce memory usage on behalf of column if its memory usage is greater
    // than or equal to |limit|.
    void release_large_columns(size_t limit) {
        LocalPool* lp = _local_pool;
        if (lp) {
            lp->release_large_columns(limit);
        }
    }

    void clear_columns() {
        LocalPool* lp = _local_pool;
        if (lp) {
            _local_pool = nullptr;
            butil::thread_atexit_cancel(LocalPool::delete_local_pool, lp);
            delete lp;
        }
    }

    ColumnPoolInfo describe_column_pool() {
        ColumnPoolInfo info;
        info.local_cnt = _nlocal.load(std::memory_order_relaxed);
        if (_free_blocks.empty()) {
            return info;
        }
        std::lock_guard<std::mutex> l(_free_blocks_lock);
        for (DynamicFreeBlock* blk : _free_blocks) {
            info.central_free_items += blk->nfree;
            info.central_free_bytes += blk->bytes;
        }
        return info;
    }

private:
    static void _release_free_block(DynamicFreeBlock* blk) {
        for (size_t i = 0; i < blk->nfree; i++) {
            T* p = blk->ptrs[i];
            ASAN_UNPOISON_MEMORY_REGION(p, sizeof(T));
            delete p;
        }
        free(blk);
    }

    LocalPool* _get_or_new_local_pool() {
        LocalPool* lp = _local_pool;
        if (LIKELY(lp != nullptr)) {
            return lp;
        }
        lp = new (std::nothrow) LocalPool(this);
        if (nullptr == lp) {
            return nullptr;
        }
        std::lock_guard<std::mutex> l(_change_thread_mutex); //avoid race with clear_columns()
        _local_pool = lp;
        (void)butil::thread_atexit(LocalPool::delete_local_pool, lp);
        _nlocal.fetch_add(1, std::memory_order_relaxed);
        return lp;
    }

    void _clear_from_destructor_of_local_pool() {
        _local_pool = nullptr;

        // Do nothing if there are active threads.
        if (_nlocal.fetch_sub(1, std::memory_order_relaxed) != 1) {
            return;
        }

        std::lock_guard<std::mutex> l(_change_thread_mutex); // including acquire fence.
        // Do nothing if there are active threads.
        if (_nlocal.load(std::memory_order_relaxed) != 0) {
            return;
        }
        // All threads exited and we're holding _change_thread_mutex to avoid
        // racing with new threads calling get_column().

        // Clear global free list.
        _first_push_time = 0;
        release_free_columns(1.0);
    }

    bool _push_free_block(const FreeBlock& blk) {
        auto* p = (DynamicFreeBlock*)malloc(offsetof(DynamicFreeBlock, ptrs) + sizeof(*blk.ptrs) * blk.nfree);
        if (UNLIKELY(p == nullptr)) {
            return false;
        }
        p->nfree = blk.nfree;
        p->bytes = blk.bytes;
        memcpy(p->ptrs, blk.ptrs, sizeof(*blk.ptrs) * blk.nfree);
        std::lock_guard<std::mutex> l(_free_blocks_lock);
        _first_push_time = _free_blocks.empty() ? butil::gettimeofday_s() : _first_push_time;
        _free_blocks.push_back(p);
        return true;
    }

    bool _pop_free_block(FreeBlock* blk) {
        if (_free_blocks.empty()) {
            return false;
        }
        _free_blocks_lock.lock();
        if (_free_blocks.empty()) {
            _free_blocks_lock.unlock();
            return false;
        }
        DynamicFreeBlock* p = _free_blocks.back();
        _free_blocks.pop_back();
        _free_blocks_lock.unlock();
        memcpy(blk->ptrs, p->ptrs, sizeof(*p->ptrs) * p->nfree);
        blk->nfree = p->nfree;
        blk->bytes = p->bytes;
        free(p);
        return true;
    }

private:
    ColumnPool() { _free_blocks.reserve(32); }

    ~ColumnPool() = default;

    std::shared_ptr<MemTracker> _mem_tracker = nullptr;

    static __thread LocalPool* _local_pool; // NOLINT
    static std::atomic<long> _nlocal;       // NOLINT
    static std::mutex _change_thread_mutex; // NOLINT

    mutable std::mutex _free_blocks_lock;
    std::vector<DynamicFreeBlock*> _free_blocks;
    int64_t _first_push_time = 0;
};

using ColumnPoolList =
        TypeList<ColumnPool<Int8Column>, ColumnPool<UInt8Column>, ColumnPool<Int16Column>, ColumnPool<Int32Column>,
                 ColumnPool<UInt32Column>, ColumnPool<Int64Column>, ColumnPool<Int128Column>, ColumnPool<FloatColumn>,
                 ColumnPool<DoubleColumn>, ColumnPool<BinaryColumn>, ColumnPool<DateColumn>,
                 ColumnPool<TimestampColumn>, ColumnPool<DecimalColumn>, ColumnPool<Decimal32Column>,
                 ColumnPool<Decimal64Column>, ColumnPool<Decimal128Column>>;

template <typename T>
__thread typename ColumnPool<T>::LocalPool* ColumnPool<T>::_local_pool = nullptr; // NOLINT

template <typename T>
std::atomic<long> ColumnPool<T>::_nlocal = 0; // NOLINT

template <typename T>
std::mutex ColumnPool<T>::_change_thread_mutex{}; // NOLINT

template <typename T, bool AllocateOnEmpty = true>
inline T* get_column() {
    static_assert(InList<ColumnPool<T>, ColumnPoolList>::value, "Cannot use column pool");
    return ColumnPool<T>::singleton()->template get_column<AllocateOnEmpty>();
}

template <typename T>
inline void return_column(T* ptr, size_t chunk_size) {
    static_assert(InList<ColumnPool<T>, ColumnPoolList>::value, "Cannot use column pool");
    ColumnPool<T>::singleton()->return_column(ptr, chunk_size);
}

template <typename T>
inline void release_large_columns(size_t limit) {
    static_assert(InList<ColumnPool<T>, ColumnPoolList>::value, "Cannot use column pool");
    ColumnPool<T>::singleton()->release_large_columns(limit);
}

template <typename T>
inline size_t release_free_columns(double ratio) {
    static_assert(InList<ColumnPool<T>, ColumnPoolList>::value, "Cannot use column pool");
    return ColumnPool<T>::singleton()->release_free_columns(ratio);
}

// NOTE: this is an expensive routine, so it should not be called too often.
template <typename T>
inline ColumnPoolInfo describe_column_pool() {
    static_assert(InList<ColumnPool<T>, ColumnPoolList>::value, "Cannot use column pool");
    return ColumnPool<T>::singleton()->describe_column_pool();
}

// Used in tests.
template <typename T>
inline void clear_columns() {
    ColumnPool<T>::singleton()->clear_columns();
}

template <typename T>
struct HasColumnPool : public std::bool_constant<InList<ColumnPool<T>, ColumnPoolList>::value> {};

namespace detail {
struct ClearColumnPool {
    template <typename Pool>
    void operator()() {
        Pool::singleton()->clear_columns();
    }
};
} // namespace detail

inline void TEST_clear_all_columns_this_thread() {
    ForEach<ColumnPoolList>(detail::ClearColumnPool());
}

} // namespace starrocks
