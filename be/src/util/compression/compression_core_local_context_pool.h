// This file is made available under Elastic License 2.0.
/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "common/status.h"
#include "common/statusor.h"
#include "util/compression/compression_context_pool.h"
#include "util/cpu_info.h"

namespace starrocks::compression {

/**
 * This class is intended to reduce contention on reserving a compression
 * context and improve cache locality (but maybe not hotness) of the contexts
 * it manages.
 *
 * This class uses CpuInfo::get_current_core() to spread the managed object across
 * NumStripes domains (which should correspond to a topologically close set of
 * hardware threads). This cache is still backed by the basic locked stack in
 * the folly::compression::CompressionContextPool.
 *
 * Note that there is a tradeoff in choosing the number of stripes. More stripes
 * make for less contention, but mean that a context is less likely to be hot
 * in cache.
 */
template <typename T, typename Creator, typename Deleter, typename Resetter, size_t NumStripes = 32>
class CompressionCoreLocalContextPool {
private:
    /**
   * Force each pointer to be on a different cache line.
   */
    class alignas(64) Storage {
    public:
        Storage() : ptr(nullptr) {}

        std::atomic<T*> ptr;
    };

    class ReturnToPoolDeleter {
    public:
        using Pool = CompressionCoreLocalContextPool<T, Creator, Deleter, Resetter, NumStripes>;

        explicit ReturnToPoolDeleter(Pool* pool) : _pool(pool) { DCHECK(_pool); }

        void operator()(T* ptr) { _pool->store(ptr); }

    private:
        Pool* _pool;
    };

    using BackingPool = CompressionContextPool<T, Creator, Deleter, Resetter>;
    using BackingPoolRef = typename BackingPool::Ref;

public:
    using Object = T;
    using Ref = std::unique_ptr<T, ReturnToPoolDeleter>;

    explicit CompressionCoreLocalContextPool(Creator creator = Creator(), Deleter deleter = Deleter(),
                                             Resetter resetter = Resetter())
            : _pool(std::move(creator), std::move(deleter), std::move(resetter)), _caches() {}

    ~CompressionCoreLocalContextPool() { flush_shallow(); }

    StatusOr<Ref> get() {
        auto ptr = local().ptr.exchange(nullptr);
        if (ptr == nullptr) {
            // no local ctx, get from backing pool
            StatusOr<BackingPoolRef> ref = _pool.get();
            Status status = ref.status();
            if (!status.ok()) {
                return status;
            }
            ptr = ref.value().release();
            DCHECK(ptr);
        }
        return Ref(ptr, get_deleter());
    }

    size_t created_count() const { return _pool.created_count(); }

    void flush_deep() {
        flush_shallow();
        _pool.flush_deep();
    }

    void flush_shallow() {
        for (auto& cache : _caches) {
            // Return all cached contexts back to the backing pool.
            auto ptr = cache.ptr.exchange(nullptr);
            return_to_backing_pool(ptr);
        }
    }

private:
    ReturnToPoolDeleter get_deleter() { return ReturnToPoolDeleter(this); }

    void store(T* ptr) {
        DCHECK(ptr);
        Status status = _pool.get_resetter()(ptr);
        // reinit fail delete this context
        if (!status.ok()) {
            delete ptr;
        }
        T* expected = nullptr;
        const bool stored = local().ptr.compare_exchange_weak(expected, ptr);
        if (!stored) {
            return_to_backing_pool(ptr);
        }
    }

    void return_to_backing_pool(T* ptr) { BackingPoolRef(ptr, _pool.get_deleter()); }

    Storage& local() {
        const auto idx = CpuInfo::get_current_core();
        return _caches[idx % NumStripes];
    }

    BackingPool _pool;
    std::array<Storage, NumStripes> _caches{};
};
} // namespace starrocks::compression
