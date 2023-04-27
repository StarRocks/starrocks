#pragma once

#include <memory>

#include "exception.hpp"
#include "method.hpp"
#include "pop.hpp"
#include "push.hpp"

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
/**
     * @ingroup wrenbind17
     * @brief Holds some Wren variable which can be a class or class instance
     * @details You can use this to pass around Wren classes or class instances.
     * You can also use this to get Wren class methods. You can also call a Wren
     * function from C++ side and pass this Variable into Wren. To get this variable,
     * either call a Wren function that returns some class (or class instance), or
     * use wrenbind17::VM::find() function that looks up a class (or class instance)
     * based on the module name.
     * @note This variable can safely outlive the wrenbind17::VM class. If that happens
     * then functions of this class will throw wrenbind17::RuntimeError exception. This
     * holder will not try to free the Wren variable if the VM has been terminated. You
     * don't have to worry about the lifetime of this holder. (uses weak pointers).
     */
class Variable {
public:
    Variable() {}
    Variable(const std::shared_ptr<Handle>& handle) : handle(handle) {}
    ~Variable() { reset(); }

    /*!
         * @brief Looks up a function from this Wren variable.
         * @details The signature must match Wren function signature.
         * For example: `main()` or `foo(_,_)` etc. Use underscores to specify
         * parameters of that function.
         * @throws RuntimeError if this variable is invalid or the Wren VM has terminated.
         */
    Method func(const std::string& signature) {
        if (const auto ptr = handle->getVmWeak().lock()) {
            auto* h = wrenMakeCallHandle(ptr.get(), signature.c_str());
            return Method(handle, std::make_shared<Handle>(ptr, h));
        } else {
            throw RuntimeError("Invalid handle");
        }
    }

    Handle& getHandle() { return *handle; }

    const Handle& getHandle() const { return *handle; }

    operator bool() const { return handle.operator bool(); }

    void reset() { handle.reset(); }

private:
    std::shared_ptr<Handle> handle;
};

template <>
inline Variable detail::getSlot<Variable>(WrenVM* vm, const int idx) {
    validate<WrenType::WREN_TYPE_UNKNOWN>(vm, idx);
    return Variable(std::make_shared<Handle>(getSharedVm(vm), wrenGetSlotHandle(vm, idx)));
}

template <>
inline bool detail::is<Variable>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WREN_TYPE_UNKNOWN;
}

template <>
inline void detail::PushHelper<Variable>::f(WrenVM* vm, int idx, const Variable& value) {
    wrenSetSlotHandle(value.getHandle().getVm(), idx, value.getHandle().getHandle());
}

template <>
inline void detail::PushHelper<Variable>::f(WrenVM* vm, int idx, Variable&& value) {
    wrenSetSlotHandle(value.getHandle().getVm(), idx, value.getHandle().getHandle());
}

template <>
inline void detail::PushHelper<const Variable>::f(WrenVM* vm, int idx, const Variable value) {
    wrenSetSlotHandle(value.getHandle().getVm(), idx, value.getHandle().getHandle());
}

template <>
inline void detail::PushHelper<const Variable&>::f(WrenVM* vm, int idx, const Variable& value) {
    wrenSetSlotHandle(value.getHandle().getVm(), idx, value.getHandle().getHandle());
}

template <>
inline void detail::PushHelper<Variable&>::f(WrenVM* vm, int idx, Variable& value) {
    wrenSetSlotHandle(value.getHandle().getVm(), idx, value.getHandle().getHandle());
}
} // namespace wrenbind17
