#pragma once

#include <deque>
#include <wren.hpp>

#include "pop.hpp"
#include "push.hpp"

namespace wrenbind17 {
#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
template <typename T>
struct PushHelper<std::deque<T>> {
    static inline void f(WrenVM* vm, int idx, std::deque<T> value) {
        if (isClassRegistered(vm, typeid(std::deque<T>).hash_code())) {
            pushAsMove<std::deque<T>>(vm, idx, std::move(value));
        } else {
            loopAndPushIterable(vm, idx, value.begin(), value.end());
        }
    }
};

template <typename T>
struct PushHelper<std::deque<T>*> {
    static inline void f(WrenVM* vm, int idx, std::deque<T>* value) {
        if (isClassRegistered(vm, typeid(std::deque<T>).hash_code())) {
            pushAsPtr<std::deque<T>>(vm, idx, value);
        } else {
            loopAndPushIterable(vm, idx, value->begin(), value->end());
        }
    }
};

template <typename T>
struct PushHelper<const std::deque<T>&> {
    static inline void f(WrenVM* vm, int idx, const std::deque<T>& value) {
        if (isClassRegistered(vm, typeid(std::deque<T>).hash_code())) {
            pushAsConstRef<std::deque<T>>(vm, idx, value);
        } else {
            loopAndPushIterable(vm, idx, value.begin(), value.end());
        }
    }
};

template <typename T>
struct PopHelper<const std::deque<T>&> {
    static inline std::deque<T> f(WrenVM* vm, const int idx) {
        const auto type = wrenGetSlotType(vm, idx);
        if (type == WrenType::WREN_TYPE_FOREIGN) {
            return *getSlotForeign<std::deque<T>>(vm, idx).get();
        }
        if (type != WrenType::WREN_TYPE_LIST) throw BadCast("Bad cast when getting value from Wren expected list");

        std::deque<T> res;
        const auto size = wrenGetListCount(vm, idx);
        wrenEnsureSlots(vm, 1);
        for (size_t i = 0; i < size; i++) {
            wrenGetListElement(vm, idx, static_cast<int>(i), idx + 1);
            res.push_back(PopHelper<T>::f(vm, idx + 1));
        }
        return res;
    }
};

template <typename T>
struct PopHelper<std::deque<T>> {
    static inline std::deque<T> f(WrenVM* vm, const int idx) { return PopHelper<std::deque<T>>::f(vm, idx); }
};
} // namespace detail
#endif
} // namespace wrenbind17
