#pragma once

#include <memory>
#include <wren.hpp>

#include "any.hpp"
#include "exception.hpp"

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
inline void pushArgs(WrenVM* vm, int idx) {
    (void)vm;
    (void)idx;
}

template <typename First, typename... Other>
inline void pushArgs(WrenVM* vm, int idx, First&& first, Other&&... other) {
    PushHelper<First>::f(vm, idx, std::forward<First>(first));
    pushArgs(vm, ++idx, std::forward<Other>(other)...);
}

template <typename... Args>
struct CallAndReturn {
    static Any func(WrenVM* vm, WrenHandle* handle, WrenHandle* func, Args&&... args) {
        constexpr auto n = sizeof...(Args);
        wrenEnsureSlots(vm, n + 1);
        wrenSetSlotHandle(vm, 0, handle);

        pushArgs(vm, 1, std::forward<Args>(args)...);

        if (wrenCall(vm, func) != WREN_RESULT_SUCCESS) {
            throw RuntimeError(getLastError(vm));
        }

        return getSlot<Any>(vm, 0);
    }
};
} // namespace detail
#endif

/**
     * @ingroup wrenbind17
     */
class Method {
public:
    Method() = default;

    Method(std::shared_ptr<Handle> variable, std::shared_ptr<Handle> handle)
            : variable(std::move(variable)), handle(std::move(handle)) {}

    ~Method() { reset(); }

    template <typename... Args>
    Any operator()(Args&&... args) {
        if (const auto ptr = handle->getVmWeak().lock().get()) {
            return detail::CallAndReturn<Args...>::func(ptr, variable->getHandle(), handle->getHandle(),
                                                        std::forward<Args>(args)...);
        } else {
            throw RuntimeError("Invalid handle");
        }
    }

    operator bool() const { return variable && handle; }

    void reset() {
        handle.reset();
        variable.reset();
    }

private:
    std::shared_ptr<Handle> variable;
    std::shared_ptr<Handle> handle;
};
} // namespace wrenbind17
