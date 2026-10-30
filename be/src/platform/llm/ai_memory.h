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

#include <cstddef>
#include <limits>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace starrocks {

// Allocation-free, type-erased memory ownership selected by an upper layer. Platform code must not depend on
// Runtime's MemTracker implementation. Logical accounting and physical allocation scope deliberately travel as one
// intrusive object, so neither half can outlive the owner independently.
class AIMemoryContext {
public:
    using Action = void (*)(void*);
    using Reserve = bool (*)(void* opaque, size_t bytes) noexcept;
    using ReleaseBytes = void (*)(void* opaque, size_t bytes) noexcept;
    using Run = void (*)(void* opaque, Action action, void* action_context);
    using Retain = void (*)(void* opaque) noexcept;
    using ReleaseOwner = void (*)(void* opaque) noexcept;

    AIMemoryContext() noexcept = default;
    ~AIMemoryContext() noexcept { _reset(); }

    AIMemoryContext(const AIMemoryContext& other) noexcept
            : _opaque(other._opaque),
              _reserve(other._reserve),
              _release_bytes(other._release_bytes),
              _run(other._run),
              _retain(other._retain),
              _release_owner(other._release_owner) {
        if (_opaque != nullptr) {
            _retain(_opaque);
        }
    }

    AIMemoryContext& operator=(const AIMemoryContext& other) noexcept {
        if (this != &other) {
            AIMemoryContext copy(other);
            swap(copy);
        }
        return *this;
    }

    AIMemoryContext(AIMemoryContext&& other) noexcept { swap(other); }

    AIMemoryContext& operator=(AIMemoryContext&& other) noexcept {
        if (this != &other) {
            AIMemoryContext moved(std::move(other));
            swap(moved);
        }
        return *this;
    }

    static AIMemoryContext create(void* opaque, Reserve reserve, ReleaseBytes release_bytes, Run run, Retain retain,
                                  ReleaseOwner release_owner) noexcept {
        if (opaque == nullptr || reserve == nullptr || release_bytes == nullptr || run == nullptr ||
            retain == nullptr || release_owner == nullptr) {
            return {};
        }
        AIMemoryContext context(opaque, reserve, release_bytes, run, retain, release_owner);
        retain(opaque);
        return context;
    }

    explicit operator bool() const noexcept { return _opaque != nullptr; }

    bool operator==(const AIMemoryContext& other) const noexcept {
        return _opaque == other._opaque && _reserve == other._reserve && _release_bytes == other._release_bytes &&
               _run == other._run && _retain == other._retain && _release_owner == other._release_owner;
    }

    bool reserve(size_t bytes) const noexcept { return _opaque == nullptr || bytes == 0 || _reserve(_opaque, bytes); }

    void release(size_t bytes) const noexcept {
        if (_opaque != nullptr && bytes != 0) {
            _release_bytes(_opaque, bytes);
        }
    }

    // The action and its context are caller-owned and normally live on the stack. Run implementations invoke the
    // action exactly once, synchronously, and propagate only an exception from that action after restoring ambient
    // state. Entering the scope performs no allocation.
    void run_in_physical_scope(Action action, void* action_context) const {
        if (action == nullptr) {
            return;
        }
        if (_opaque == nullptr) {
            action(action_context);
            return;
        }
        _run(_opaque, action, action_context);
    }

    void swap(AIMemoryContext& other) noexcept {
        std::swap(_opaque, other._opaque);
        std::swap(_reserve, other._reserve);
        std::swap(_release_bytes, other._release_bytes);
        std::swap(_run, other._run);
        std::swap(_retain, other._retain);
        std::swap(_release_owner, other._release_owner);
    }

private:
    AIMemoryContext(void* opaque, Reserve reserve, ReleaseBytes release_bytes, Run run, Retain retain,
                    ReleaseOwner release_owner) noexcept
            : _opaque(opaque),
              _reserve(reserve),
              _release_bytes(release_bytes),
              _run(run),
              _retain(retain),
              _release_owner(release_owner) {}

    void _reset() noexcept {
        void* opaque = std::exchange(_opaque, nullptr);
        if (opaque != nullptr) {
            _release_owner(opaque);
        }
        _reserve = nullptr;
        _release_bytes = nullptr;
        _run = nullptr;
        _retain = nullptr;
        _release_owner = nullptr;
    }

    void* _opaque = nullptr;
    Reserve _reserve = nullptr;
    ReleaseBytes _release_bytes = nullptr;
    Run _run = nullptr;
    Retain _retain = nullptr;
    ReleaseOwner _release_owner = nullptr;
};

inline void swap(AIMemoryContext& lhs, AIMemoryContext& rhs) noexcept {
    lhs.swap(rhs);
}

// Allocator for physically charging asynchronous request-owned objects to the request context. Admission control
// objects are not logical payload, so this allocator deliberately never calls reserve()/release().
template <typename T>
class AIMemoryContextAllocator {
public:
    using value_type = T;
    using propagate_on_container_move_assignment = std::true_type;
    using is_always_equal = std::false_type;

    template <typename U>
    struct rebind {
        using other = AIMemoryContextAllocator<U>;
    };

    AIMemoryContextAllocator() noexcept = default;
    explicit AIMemoryContextAllocator(AIMemoryContext memory) noexcept : _memory(std::move(memory)) {}

    template <typename U>
    AIMemoryContextAllocator(const AIMemoryContextAllocator<U>& other) noexcept : _memory(other._memory) {}

    [[nodiscard]] T* allocate(size_t count) {
        if (count > max_size()) {
            throw std::bad_array_new_length();
        }
        T* result = nullptr;
        _run_in_physical_scope([&] { result = std::allocator<T>{}.allocate(count); });
        return result;
    }

    void deallocate(T* pointer, size_t count) noexcept {
        _run_in_physical_scope([&] { std::allocator<T>{}.deallocate(pointer, count); });
    }

    template <typename U, typename... Args>
    void construct(U* pointer, Args&&... args) {
        _run_in_physical_scope([&] { std::construct_at(pointer, std::forward<Args>(args)...); });
    }

    template <typename U>
    void destroy(U* pointer) noexcept {
        _run_in_physical_scope([&] { std::destroy_at(pointer); });
    }

    constexpr size_t max_size() const noexcept { return std::numeric_limits<size_t>::max() / sizeof(T); }

    template <typename U>
    bool operator==(const AIMemoryContextAllocator<U>& other) const noexcept {
        return _memory == other._memory;
    }

private:
    template <typename>
    friend class AIMemoryContextAllocator;

    template <typename Function>
    void _run_in_physical_scope(Function&& function) const {
        using StoredFunction = std::remove_reference_t<Function>;
        _memory.run_in_physical_scope([](void* opaque) { (*static_cast<StoredFunction*>(opaque))(); },
                                      std::addressof(function));
    }

    AIMemoryContext _memory;
};

template <typename T, typename... Args>
std::shared_ptr<T> ai_allocate_shared(const AIMemoryContext& memory, Args&&... args) {
    return std::allocate_shared<T>(AIMemoryContextAllocator<T>(memory), std::forward<Args>(args)...);
}

} // namespace starrocks
