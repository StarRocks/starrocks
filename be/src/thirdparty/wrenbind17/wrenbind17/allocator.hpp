#pragma once

#include <string>
#include <wren.hpp>

#include "index.hpp"
#include "pop.hpp"
#include "push.hpp"

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
void setNextError(WrenVM* vm, std::string str);

#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
template <typename T, typename... Args>
struct ForeignKlassAllocator {
    static T* ctor(Args&&... args) { return new T(std::forward<Args>(args)...); }
    template <size_t... Is>
    static T* ctorFrom(WrenVM* vm, detail::index_list<Is...>) {
        return ctor(PopHelper<Args>::f(vm, Is + 1)...);
    }
    static void allocate(WrenVM* vm) {
        auto* memory = wrenSetSlotNewForeign(vm, 0, 0, sizeof(ForeignObject<T>));
        new (memory) ForeignObject<T>();
        auto* wrapper = reinterpret_cast<ForeignObject<T>*>(memory);
        try {
            wrapper->ptr.reset(ctorFrom(vm, detail::index_range<0, sizeof...(Args)>()));
        } catch (std::exception& e) {
            wrenEnsureSlots(vm, 1);
            wrenSetSlotString(vm, 0, e.what());
            wrenAbortFiber(vm, 0);
        }
    }
    static void finalize(void* memory) {
        auto* wrapper = reinterpret_cast<ForeignObject<T>*>(memory);
        wrapper->~ForeignObject<T>();
    }
};
} // namespace detail
#endif
} // namespace wrenbind17
