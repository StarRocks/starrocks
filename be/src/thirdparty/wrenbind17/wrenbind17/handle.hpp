#pragma once

#include <memory>
#include <wren.hpp>

#include "exception.hpp"

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
std::shared_ptr<WrenVM> getSharedVm(WrenVM* vm);

/**
     * @ingroup wrenbind17
     * @brief Holds a reference to some Wren type
     * @details This is used by Map, Method, and Variable classes.
     */
class Handle {
public:
    Handle() : handle(nullptr) {}
    Handle(const std::shared_ptr<WrenVM> vm, WrenHandle* handle) : vm(vm), handle(handle) {}
    ~Handle() { reset(); }
    Handle(const Handle& other) = delete;
    Handle(Handle&& other) noexcept : handle(nullptr) { swap(other); }
    Handle& operator=(const Handle& other) = delete;
    Handle& operator=(Handle&& other) noexcept {
        if (this != &other) {
            swap(other);
        }
        return *this;
    }
    void swap(Handle& other) noexcept {
        std::swap(vm, other.vm);
        std::swap(handle, other.handle);
    }

    WrenHandle* getHandle() const { return handle; }

    WrenVM* getVm() const {
        if (const auto ptr = vm.lock()) {
            return ptr.get();
        } else {
            throw RuntimeError("Invalid handle");
        }
    }

    const std::weak_ptr<WrenVM>& getVmWeak() const { return vm; }

    void reset() {
        if (!vm.expired() && handle) {
            wrenReleaseHandle(vm.lock().get(), handle);
            vm.reset();
            handle = nullptr;
        }
    }

    operator bool() const { return !vm.expired() && handle; }

private:
    std::weak_ptr<WrenVM> vm;
    WrenHandle* handle;
};
} // namespace wrenbind17
