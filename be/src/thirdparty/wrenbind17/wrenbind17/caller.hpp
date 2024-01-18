#pragma once

#include <memory>
#include <wren.hpp>

#include "index.hpp"
#include "pop.hpp"
#include "push.hpp"

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
template <typename R>
struct ForeginMethodReturnHelper {
    static inline void push(WrenVM* vm, int index, R ret) { PushHelper<R>::f(vm, index, ret); }
};

template <typename R>
struct ForeginMethodReturnHelper<R&> {
    static inline void push(WrenVM* vm, int index, R& ret) { PushHelper<R*>::f(vm, index, &ret); }
};

template <typename R>
struct ForeginMethodReturnHelper<const R&> {
    static inline void push(WrenVM* vm, int index, const R& ret) { PushHelper<R*>::f(vm, index, &const_cast<R&>(ret)); }
};

template <typename R>
struct ForeginMethodReturnHelper<R*> {
    static inline void push(WrenVM* vm, int index, R* ret) { PushHelper<R*>::f(vm, index, ret); }
};

template <typename R>
struct ForeginMethodReturnHelper<const R*> {
    static inline void push(WrenVM* vm, int index, const R* ret) { PushHelper<const R*>::f(vm, index, ret); }
};

template <typename R>
struct ForeginMethodReturnHelper<R&&> {
    static inline void push(WrenVM* vm, int index, R&& ret) { PushHelper<R>::f(vm, index, std::move(ret)); }
};

template <typename R>
struct ForeginMethodReturnHelper<const std::shared_ptr<R>&> {
    static inline void push(WrenVM* vm, int index, const std::shared_ptr<R>& ret) {
        PushHelper<const std::shared_ptr<R>&>::f(vm, index, ret);
    }
};

template <typename R>
struct ForeginMethodReturnHelper<std::shared_ptr<R>&> {
    static inline void push(WrenVM* vm, int index, std::shared_ptr<R>& ret) {
        PushHelper<std::shared_ptr<R>&>::f(vm, index, ret);
    }
};

template <typename R>
struct ForeginMethodReturnHelper<const std::variant<R>&> {
    static inline void push(WrenVM* vm, int index, const std::variant<R>& ret) {
        PushHelper<const std::variant<R>&>::f(vm, index, ret);
    }
};

template <typename R>
struct ForeginMethodReturnHelper<std::variant<R>&> {
    static inline void push(WrenVM* vm, int index, std::variant<R>& ret) {
        PushHelper<std::variant<R>&>::f(vm, index, ret);
    }
};

template <>
inline void ForeginMethodReturnHelper<const std::string&>::push(WrenVM* vm, int index, const std::string& ret) {
    PushHelper<const std::string&>::f(vm, index, ret);
}

template <>
inline void ForeginMethodReturnHelper<std::string&>::push(WrenVM* vm, int index, std::string& ret) {
    PushHelper<std::string&>::f(vm, index, ret);
}

template <typename R, typename T, typename... Args>
struct ForeignMethodCaller {
    template <R (T::*Fn)(Args...), size_t... Is>
    static void callFrom(WrenVM* vm, detail::index_list<Is...>) {
        auto self = PopHelper<T*>::f(vm, 0);
<<<<<<< HEAD
=======
        if (self == nullptr) {
            throw std::runtime_error("null pointer");
        }
>>>>>>> 2.5.18
        // R ret = (self->*Fn)(PopHelper<typename std::remove_const<Args>::type>::f(vm, Is + 1)...);
        // PushHelper<R>::f(vm, 0, ret);
        ForeginMethodReturnHelper<R>::push(
                vm, 0, (self->*Fn)(PopHelper<typename std::remove_const<Args>::type>::f(vm, Is + 1)...));
    }

    template <R (T::*Fn)(Args...)>
    static void call(WrenVM* vm) {
        try {
            callFrom<Fn>(vm, detail::index_range<0, sizeof...(Args)>());
        } catch (...) {
            exceptionHandler(vm, std::current_exception());
        }
    }

    template <R (T::*Fn)(Args...) const, size_t... Is>
    static void callFrom(WrenVM* vm, detail::index_list<Is...>) {
        auto self = PopHelper<T*>::f(vm, 0);
<<<<<<< HEAD
=======
        if (self == nullptr) {
            throw std::runtime_error("null pointer");
        }
>>>>>>> 2.5.18
        // R ret = (self->*Fn)(PopHelper<typename std::remove_const<Args>::type>::f(vm, Is + 1)...);
        // PushHelper<R>::f(vm, 0, ret);
        ForeginMethodReturnHelper<R>::push(
                vm, 0, (self->*Fn)(PopHelper<typename std::remove_const<Args>::type>::f(vm, Is + 1)...));
    }

    template <R (T::*Fn)(Args...) const>
    static void call(WrenVM* vm) {
        try {
            callFrom<Fn>(vm, detail::index_range<0, sizeof...(Args)>());
        } catch (...) {
            exceptionHandler(vm, std::current_exception());
        }
    }
};

template <typename T, typename... Args>
struct ForeignMethodCaller<void, T, Args...> {
    template <void (T::*Fn)(Args...), size_t... Is>
    static void callFrom(WrenVM* vm, detail::index_list<Is...>) {
        auto self = PopHelper<T*>::f(vm, 0);
<<<<<<< HEAD
=======
        if (self == nullptr) {
            throw std::runtime_error("null pointer");
        }
>>>>>>> 2.5.18
        (self->*Fn)(PopHelper<typename std::remove_const<Args>::type>::f(vm, Is + 1)...);
    }

    template <void (T::*Fn)(Args...)>
    static void call(WrenVM* vm) {
        try {
            callFrom<Fn>(vm, detail::index_range<0, sizeof...(Args)>());
        } catch (...) {
            exceptionHandler(vm, std::current_exception());
        }
    }

    template <void (T::*Fn)(Args...) const, size_t... Is>
    static void callFrom(WrenVM* vm, detail::index_list<Is...>) {
        auto self = PopHelper<T*>::f(vm, 0);
<<<<<<< HEAD
=======
        if (self == nullptr) {
            throw std::runtime_error("null pointer");
        }
>>>>>>> 2.5.18
        (self->*Fn)(PopHelper<typename std::remove_const<Args>::type>::f(vm, Is + 1)...);
    }

    template <void (T::*Fn)(Args...) const>
    static void call(WrenVM* vm) {
        try {
            callFrom<Fn>(vm, detail::index_range<0, sizeof...(Args)>());
        } catch (...) {
            exceptionHandler(vm, std::current_exception());
        }
    }
};

template <typename R, typename T, typename... Args>
struct ForeignMethodExtCaller {
    template <R (*Fn)(T&, Args...), size_t... Is>
    static void callFrom(WrenVM* vm, detail::index_list<Is...>) {
        auto self = PopHelper<T*>::f(vm, 0);
<<<<<<< HEAD
=======
        if (self == nullptr) {
            throw std::runtime_error("null pointer");
        }
>>>>>>> 2.5.18
        // R ret = (*Fn)(*self, PopHelper<typename std::remove_const<Args>::type>::f(vm, Is + 1)...);
        // PushHelper<R>::f(vm, 0, ret);
        ForeginMethodReturnHelper<R>::push(
                vm, 0, (*Fn)(*self, PopHelper<typename std::remove_const<Args>::type>::f(vm, Is + 1)...));
    }

    template <R (*Fn)(T&, Args...)>
    static void call(WrenVM* vm) {
        try {
            callFrom<Fn>(vm, detail::index_range<0, sizeof...(Args)>());
        } catch (...) {
            exceptionHandler(vm, std::current_exception());
        }
    }
};

template <typename T, typename... Args>
struct ForeignMethodExtCaller<void, T, Args...> {
    template <void (*Fn)(T&, Args...), size_t... Is>
    static void callFrom(WrenVM* vm, detail::index_list<Is...>) {
        auto self = PopHelper<T*>::f(vm, 0);
<<<<<<< HEAD
=======
        if (self == nullptr) {
            throw std::runtime_error("null pointer");
        }
>>>>>>> 2.5.18
        (*Fn)(*self, PopHelper<typename std::remove_const<Args>::type>::f(vm, Is + 1)...);
    }

    template <void (*Fn)(T&, Args...)>
    static void call(WrenVM* vm) {
        try {
            callFrom<Fn>(vm, detail::index_range<0, sizeof...(Args)>());
        } catch (...) {
            exceptionHandler(vm, std::current_exception());
        }
    }
};

template <typename R, typename... Args>
struct ForeignFunctionCaller {
    template <R (*Fn)(Args...), size_t... Is>
    static void callFrom(WrenVM* vm, detail::index_list<Is...>) {
        // R ret = (*Fn)(PopHelper<typename std::remove_const<Args>::type>::f(vm, Is + 1)...);
        // PushHelper<R>::f(vm, 0, ret);
        ForeginMethodReturnHelper<R>::push(vm, 0,
                                           (*Fn)(PopHelper<typename std::remove_const<Args>::type>::f(vm, Is + 1)...));
    }

    template <R (*Fn)(Args...)>
    static void call(WrenVM* vm) {
        try {
            callFrom<Fn>(vm, detail::index_range<0, sizeof...(Args)>());
        } catch (...) {
            exceptionHandler(vm, std::current_exception());
        }
    }
};

template <typename... Args>
struct ForeignFunctionCaller<void, Args...> {
    template <void (*Fn)(Args...), size_t... Is>
    static void callFrom(WrenVM* vm, detail::index_list<Is...>) {
        (*Fn)(PopHelper<typename std::remove_const<Args>::type>::f(vm, Is + 1)...);
    }

    template <void (*Fn)(Args...)>
    static void call(WrenVM* vm) {
        try {
            callFrom<Fn>(vm, detail::index_range<0, sizeof...(Args)>());
        } catch (...) {
            exceptionHandler(vm, std::current_exception());
        }
    }
};

template <typename T, typename V, V T::*Ptr>
struct ForeignPropCaller {
    static void setter(WrenVM* vm) {
        auto self = PopHelper<T*>::f(vm, 0);
<<<<<<< HEAD
=======
        if (self == nullptr) {
            throw std::runtime_error("null pointer");
        }
>>>>>>> 2.5.18
        self->*Ptr = PopHelper<V>::f(vm, 1);
    }

    static void getter(WrenVM* vm) {
        auto self = PopHelper<T*>::f(vm, 0);
<<<<<<< HEAD
=======
        if (self == nullptr) {
            throw std::runtime_error("null pointer");
        }
>>>>>>> 2.5.18
        PushHelper<V>::f(vm, 0, std::forward<decltype(self->*Ptr)>(self->*Ptr));
    }
};
} // namespace detail
#endif
} // namespace wrenbind17
