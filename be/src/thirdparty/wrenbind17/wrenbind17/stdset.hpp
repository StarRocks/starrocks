#pragma once

#include <set>
#include <unordered_set>
#include <wren.hpp>

#include "pop.hpp"
#include "push.hpp"

namespace wrenbind17 {
#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
template <typename T>
struct PushHelper<std::set<T>> {
    static inline void f(WrenVM* vm, int idx, std::set<T> value) {
        if (isClassRegistered(vm, typeid(std::set<T>).hash_code())) {
            pushAsMove<std::vector<T>>(vm, idx, std::move(value));
        } else {
            loopAndPushIterable(vm, idx, value.begin(), value.end());
        }
    }
};

template <typename T>
struct PushHelper<std::set<T>*> {
    static inline void f(WrenVM* vm, int idx, std::set<T>* value) {
        if (isClassRegistered(vm, typeid(std::set<T>).hash_code())) {
            pushAsPtr<std::set<T>>(vm, idx, value);
        } else {
            loopAndPushIterable(vm, idx, value->begin(), value->end());
        }
    }
};

template <typename T>
struct PushHelper<const std::set<T>&> {
    static inline void f(WrenVM* vm, int idx, const std::set<T>& value) {
        if (isClassRegistered(vm, typeid(std::set<T>).hash_code())) {
            pushAsConstRef<std::set<T>>(vm, idx, value);
        } else {
            loopAndPushIterable(vm, idx, value.begin(), value.end());
        }
    }
};

template <typename T>
struct PopHelper<const std::set<T>&> {
    static inline std::set<T> f(WrenVM* vm, const int idx) {
        const auto type = wrenGetSlotType(vm, idx);
        if (type == WrenType::WREN_TYPE_FOREIGN) {
            return *getSlotForeign<std::set<T>>(vm, idx).get();
        }
        if (type != WrenType::WREN_TYPE_LIST) throw BadCast("Bad cast when getting value from Wren expected list");

        std::set<T> res;
        const auto size = wrenGetListCount(vm, idx);
        wrenEnsureSlots(vm, 1);
        res.reserve(size);
        for (size_t i = 0; i < size; i++) {
            wrenGetListElement(vm, idx, static_cast<int>(i), idx + 1);
            res.insert(PopHelper<T>::f(vm, idx + 1));
        }
        return res;
    }
};

template <typename T>
struct PopHelper<std::set<T>> {
    static inline std::set<T> f(WrenVM* vm, const int idx) { return PopHelper<const std::set<T>&>::f(vm, idx); }
};

template <typename T>
struct PushHelper<std::unordered_set<T>> {
    static inline void f(WrenVM* vm, int idx, std::unordered_set<T> value) {
        if (isClassRegistered(vm, typeid(std::unordered_set<T>).hash_code())) {
            pushAsMove<std::vector<T>>(vm, idx, std::move(value));
        } else {
            loopAndPushIterable(vm, idx, value.begin(), value.end());
        }
    }
};

template <typename T>
struct PushHelper<std::unordered_set<T>*> {
    static inline void f(WrenVM* vm, int idx, std::unordered_set<T>* value) {
        if (isClassRegistered(vm, typeid(std::unordered_set<T>).hash_code())) {
            pushAsPtr<std::set<T>>(vm, idx, value);
        } else {
            loopAndPushIterable(vm, idx, value->begin(), value->end());
        }
    }
};

template <typename T>
struct PushHelper<const std::unordered_set<T>&> {
    static inline void f(WrenVM* vm, int idx, const std::unordered_set<T>& value) {
        if (isClassRegistered(vm, typeid(std::unordered_set<T>).hash_code())) {
            pushAsConstRef<std::set<T>>(vm, idx, value);
        } else {
            loopAndPushIterable(vm, idx, value.begin(), value.end());
        }
    }
};

template <typename T>
struct PopHelper<const std::unordered_set<T>&> {
    static inline std::set<T> f(WrenVM* vm, const int idx) {
        const auto type = wrenGetSlotType(vm, idx);
        if (type == WrenType::WREN_TYPE_FOREIGN) {
            return *getSlotForeign<std::unordered_set<T>>(vm, idx).get();
        }
        if (type != WrenType::WREN_TYPE_LIST) throw BadCast("Bad cast when getting value from Wren expected list");

        std::unordered_set<T> res;
        const auto size = wrenGetListCount(vm, idx);
        wrenEnsureSlots(vm, 1);
        res.reserve(size);
        for (size_t i = 0; i < size; i++) {
            wrenGetListElement(vm, idx, static_cast<int>(i), idx + 1);
            res.insert(PopHelper<T>::f(vm, idx + 1));
        }
        return res;
    }
};

template <typename T>
struct PopHelper<std::unordered_set<T>> {
    static inline std::set<T> f(WrenVM* vm, const int idx) {
        return PopHelper<const std::unordered_set<T>&>::f(vm, idx);
    }
};
} // namespace detail
#endif
} // namespace wrenbind17
