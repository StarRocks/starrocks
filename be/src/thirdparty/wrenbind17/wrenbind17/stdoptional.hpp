#pragma once

#include <optional>
#include <wren.hpp>

#include "pop.hpp"
#include "push.hpp"

namespace wrenbind17 {
#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
template <typename T>
struct PushHelper<std::optional<T>> {
    inline static void f(WrenVM* vm, int idx, const std::optional<T>& value) {
        if (value.has_value()) {
            PushHelper<T>::f(vm, idx, value.value());
        } else {
            PushHelper<std::nullptr_t>::f(vm, idx, nullptr);
        }
    }
};

template <typename T>
struct PushHelper<std::optional<T>&> {
    inline static void f(WrenVM* vm, int idx, const std::optional<T>& value) {
        PushHelper<std::optional<T>>::f(vm, idx, value);
    }
};

template <typename T>
struct PushHelper<std::optional<T>*> {
    inline static void f(WrenVM* vm, int idx, const std::optional<T>* value) {
        PushHelper<std::optional<T>>::f(vm, idx, *value);
    }
};

template <typename T>
struct PushHelper<const std::optional<T>&> {
    inline static void f(WrenVM* vm, int idx, const std::optional<T>& value) {
        PushHelper<std::optional<T>>::f(vm, idx, value);
    }
};

template <typename T>
struct PopHelper<std::optional<T>> {
    static inline std::optional<T> f(WrenVM* vm, const int idx) {
        if (is<std::nullptr_t>(vm, idx)) {
            return std::nullopt;
        } else {
            return PopHelper<T>::f(vm, idx);
        }
    }
};

template <typename T>
struct PopHelper<const std::optional<T>&> {
    static inline std::optional<T> f(WrenVM* vm, const int idx) { return PopHelper<std::optional<T>>::f(vm, idx); }
};
} // namespace detail
#endif
} // namespace wrenbind17
