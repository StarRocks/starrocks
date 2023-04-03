#pragma once

#include <variant>
#include <wren.hpp>

#include "pop.hpp"
#include "push.hpp"

namespace wrenbind17 {
#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
template <typename VariantType>
inline void loopAndPushVariant(WrenVM* vm, int idx, const VariantType& v, size_t i) {
    PushHelper<std::nullptr_t>::f(vm, idx, nullptr);
}

template <typename VariantType, typename T, typename... Ts>
inline void loopAndPushVariant(WrenVM* vm, int idx, const VariantType& v, size_t i) {
    if (v.index() == i) {
        PushHelper<T>::f(vm, idx, std::get<T>(v));
    } else {
        loopAndPushVariant<VariantType, Ts...>(vm, idx, v, i + 1);
    }
}

template <typename... Ts>
struct PushHelper<std::variant<Ts...>> {
    inline static void f(WrenVM* vm, int idx, const std::variant<Ts...>& value) {
        loopAndPushVariant<std::variant<Ts...>, Ts...>(vm, idx, value, 0);
    }
};

template <typename... Ts>
struct PushHelper<std::variant<Ts...>&> {
    inline static void f(WrenVM* vm, int idx, const std::variant<Ts...>& value) {
        PushHelper<std::variant<Ts...>>::f(vm, idx, value);
    }
};

template <typename... Ts>
struct PushHelper<std::variant<Ts...>*> {
    inline static void f(WrenVM* vm, int idx, const std::variant<Ts...>* value) {
        PushHelper<std::variant<Ts...>>::f(vm, idx, *value);
    }
};

template <typename... Ts>
struct PushHelper<const std::variant<Ts...>&> {
    inline static void f(WrenVM* vm, int idx, const std::variant<Ts...>& value) {
        PushHelper<std::variant<Ts...>>::f(vm, idx, value);
    }
};

template <typename VariantType>
VariantType loopAndFindVariant(WrenVM* vm, int idx) {
    throw BadCast("Bad cast when getting variant from Wren");
}

template <typename VariantType, typename T, typename... Ts>
VariantType loopAndFindVariant(WrenVM* vm, const int idx) {
    if (CheckSlot<T>::f(vm, idx)) {
        return {PopHelper<T>::f(vm, idx)};
    }
    return loopAndFindVariant<VariantType, Ts...>(vm, idx);
}

template <typename... Ts>
struct PopHelper<std::variant<Ts...>> {
    static inline std::variant<Ts...> f(WrenVM* vm, const int idx) {
        using VariantType = typename std::variant<Ts...>;
        return loopAndFindVariant<VariantType, Ts...>(vm, idx);
    }
};

template <typename... Ts>
struct PopHelper<const std::variant<Ts...>&> {
    static inline std::variant<Ts...> f(WrenVM* vm, const int idx) {
        using VariantType = typename std::variant<Ts...>;
        return loopAndFindVariant<VariantType, Ts...>(vm, idx);
    }
};
} // namespace detail
#endif
} // namespace wrenbind17
