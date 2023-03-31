#pragma once

#include <map>
#include <unordered_map>
#include <wren.hpp>

#include "pop.hpp"
#include "push.hpp"

namespace wrenbind17 {
#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
template <typename T>
struct PushHelper<std::map<std::string, T>> {
    static inline void f(WrenVM* vm, int idx, std::map<std::string, T> value) {
        if (isClassRegistered(vm, typeid(std::map<std::string, T>).hash_code())) {
            pushAsMove<std::map<std::string, T>>(vm, idx, std::move(value));
        } else {
            loopAndPushKeyPair(vm, idx, value.begin(), value.end());
        }
    }
};

template <typename T>
struct PushHelper<std::map<std::string, T>*> {
    static inline void f(WrenVM* vm, int idx, std::map<std::string, T>* value) {
        if (isClassRegistered(vm, typeid(std::map<std::string, T>).hash_code())) {
            pushAsPtr<std::map<std::string, T>>(vm, idx, value);
        } else {
            loopAndPushKeyPair(vm, idx, value->begin(), value->end());
        }
    }
};

template <typename T>
struct PushHelper<const std::map<std::string, T>&> {
    static inline void f(WrenVM* vm, int idx, const std::map<std::string, T>& value) {
        if (isClassRegistered(vm, typeid(std::map<std::string, T>).hash_code())) {
            pushAsConstRef<std::map<std::string, T>>(vm, idx, value);
        } else {
            loopAndPushKeyPair(vm, idx, value.begin(), value.end());
        }
    }
};

template <typename T>
struct PushHelper<std::unordered_map<std::string, T>> {
    static inline void f(WrenVM* vm, int idx, std::unordered_map<std::string, T> value) {
        if (isClassRegistered(vm, typeid(std::unordered_map<std::string, T>).hash_code())) {
            pushAsMove<std::unordered_map<std::string, T>>(vm, idx, std::move(value));
        } else {
            loopAndPushKeyPair(vm, idx, value.begin(), value.end());
        }
    }
};

template <typename T>
struct PushHelper<std::unordered_map<std::string, T>*> {
    static inline void f(WrenVM* vm, int idx, std::unordered_map<std::string, T>* value) {
        if (isClassRegistered(vm, typeid(std::unordered_map<std::string, T>).hash_code())) {
            pushAsPtr<std::unordered_map<std::string, T>>(vm, idx, value);
        } else {
            loopAndPushKeyPair(vm, idx, value->begin(), value->end());
        }
    }
};

template <typename T>
struct PushHelper<const std::unordered_map<std::string, T>&> {
    static inline void f(WrenVM* vm, int idx, const std::unordered_map<std::string, T>& value) {
        if (isClassRegistered(vm, typeid(std::unordered_map<std::string, T>).hash_code())) {
            pushAsConstRef<std::unordered_map<std::string, T>>(vm, idx, value);
        } else {
            loopAndPushKeyPair(vm, idx, value.begin(), value.end());
        }
    }
};
} // namespace detail
#endif
} // namespace wrenbind17
