#pragma once

#include <vector>
#include <wren.hpp>

#include "pop.hpp"
#include "push.hpp"

namespace wrenbind17 {
#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
template <typename T>
struct PushHelper<std::vector<T>> {
    static inline void f(WrenVM* vm, int idx, std::vector<T> value) {
        if (isClassRegistered(vm, typeid(std::vector<T>).hash_code())) {
            pushAsMove<std::vector<T>>(vm, idx, std::move(value));
        } else {
            loopAndPushIterable(vm, idx, value.begin(), value.end());
        }
    }
};

template <typename T>
struct PushHelper<std::vector<T>*> {
    static inline void f(WrenVM* vm, int idx, std::vector<T>* value) {
        if (isClassRegistered(vm, typeid(std::vector<T>).hash_code())) {
            pushAsPtr<std::vector<T>>(vm, idx, value);
        } else {
            loopAndPushIterable(vm, idx, value->begin(), value->end());
        }
    }
};

template <typename T>
struct PushHelper<const std::vector<T>&> {
    static inline void f(WrenVM* vm, int idx, const std::vector<T>& value) {
        if (isClassRegistered(vm, typeid(std::vector<T>).hash_code())) {
            pushAsConstRef<std::vector<T>>(vm, idx, value);
        } else {
            loopAndPushIterable(vm, idx, value.begin(), value.end());
        }
    }
};

template <typename T>
struct PopHelper<const std::vector<T>&> {
    static inline std::vector<T> f(WrenVM* vm, const int idx) {
        const auto type = wrenGetSlotType(vm, idx);
        if (type == WrenType::WREN_TYPE_FOREIGN) {
            return *getSlotForeign<std::vector<T>>(vm, idx).get();
        }
        if (type != WrenType::WREN_TYPE_LIST) throw BadCast("Bad cast when getting value from Wren expected list");

        std::vector<T> res;
        const auto size = wrenGetListCount(vm, idx);
        wrenEnsureSlots(vm, 1);
        res.reserve(size);
        for (size_t i = 0; i < size; i++) {
            wrenGetListElement(vm, idx, static_cast<int>(i), idx + 1);
            res.push_back(PopHelper<T>::f(vm, idx + 1));
        }
        return res;
    }
};

template <typename T>
struct PopHelper<std::vector<T>> {
    static inline std::vector<T> f(WrenVM* vm, const int idx) { return PopHelper<const std::vector<T>&>::f(vm, idx); }
};
} // namespace detail
#endif
} // namespace wrenbind17
