#pragma once

#include <memory>
#include <typeinfo>

#include "handle.hpp"
#include "pop.hpp"
#include "push.hpp"

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
/**
     * @ingroup wrenbind17
     * @brief A return value when calling a Wren function (alias Any)
     * @see Any
     * @details This extends the lifetime of the Wren object (handle). As long as
     * this ReturnValue instance exists the Wren object will exist.
     * @note This variable can safely outlive the wrenbind17::VM class. If that happens
     * then functions of this class will throw wrenbind17::RuntimeError exception. This
     * holder will not try to free the Wren variable if the VM has been terminated. You
     * don't have to worry about the lifetime of this holder. (uses weak pointers).
     */
class ReturnValue {
public:
    ReturnValue() = default;
    explicit ReturnValue(const WrenType type, Handle handle) : type(type), handle(std::move(handle)) {}
    ~ReturnValue() = default;

    ReturnValue(const ReturnValue& other) = delete;
    ReturnValue(ReturnValue&& other) noexcept { swap(other); }

    ReturnValue& operator=(const ReturnValue& other) = delete;
    ReturnValue& operator=(ReturnValue&& other) noexcept {
        if (this != &other) {
            swap(other);
        }
        return *this;
    }

    void swap(ReturnValue& other) noexcept {
        std::swap(type, other.type);
        std::swap(handle, other.handle);
    }

    /*!
         * Returns the handle that this instance owns
         */
    const Handle& getHandle() const { return handle; }

    /*!
         * Returns the handle that this instance owns
         */
    Handle& getHandle() { return handle; }

    /*!
         * @brief The raw wren type held by this instance
         */
    WrenType getType() const { return type; }

    /*!
         * @brief Check if the value held is some specific C++ type
         * @note If the value held is a Wren numeric type then checking for any
         * C++ integral or floating type will return true.
         */
    template <class T>
    bool is() const {
        if (type == WREN_TYPE_NULL) {
            return false;
        }
        if (const auto vm = handle.getVmWeak().lock()) {
            wrenEnsureSlots(vm.get(), 1);
            wrenSetSlotHandle(vm.get(), 0, handle.getHandle());
            using Type = typename std::remove_reference<typename std::remove_pointer<T>::type>::type;
            return detail::is<Type>(vm.get(), 0);
        } else {
            throw RuntimeError("Invalid handle");
        }
    }

    bool isMap() const { return type == WREN_TYPE_MAP; }

    bool isList() const { return type == WREN_TYPE_LIST; }

    /*!
         * @brief Returns the value
         * @note If the value held is a Wren numeric type then getting for any
         * C++ integral or floating type will result in a cast from a double to that type.
         * @throws RuntimeError if this instance is invalid (constructed via the default constructor)
         * @throws BadCast if the type required by specifying the template argument does not match the type held
         * @tparam T the type you want to get
         * @see shared()
         */
    template <class T>
    T as() {
        if (type == WREN_TYPE_NULL) {
            throw BadCast("Bad cast when getting value from Wren");
        }
        if (const auto vm = handle.getVmWeak().lock()) {
            wrenEnsureSlots(vm.get(), 1);
            wrenSetSlotHandle(vm.get(), 0, handle.getHandle());
            return detail::PopHelper<T>::f(vm.get(), 0);
        } else {
            throw RuntimeError("Invalid handle");
        }
    }

    /*!
         * @brief Returns the value as a shared pointer
         * @note Only works for custom C++ classes (foreign classes) that have been bound to the VM.
         * @throws RuntimeError if this instance is invalid (constructed via the default constructor)
         * @throws BadCast if the type required by specifying the template argument does not match the type held
         * @tparam T the type of the std::shared_ptr you want to get
         * @see as()
         */
    template <class T>
    std::shared_ptr<T> shared() {
        return as<std::shared_ptr<T>>();
    }

private:
    WrenType type = WrenType::WREN_TYPE_NULL;
    Handle handle;
};

#ifndef DOXYGEN_SHOULD_SKIP_THIS
template <>
inline bool ReturnValue::is<int8_t>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<char>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<short>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<int>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<long>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<long long>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<unsigned char>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<unsigned short>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<unsigned int>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<unsigned long>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<unsigned long long>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<float>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<double>() const {
    return type == WREN_TYPE_NUM;
}

template <>
inline bool ReturnValue::is<bool>() const {
    return type == WREN_TYPE_BOOL;
}

template <>
inline bool ReturnValue::is<std::nullptr_t>() const {
    return type == WREN_TYPE_NULL;
}

template <>
inline bool ReturnValue::is<std::string>() const {
    return type == WREN_TYPE_STRING;
}

template <>
inline std::nullptr_t ReturnValue::as<std::nullptr_t>() {
    if (!is<std::nullptr_t>()) {
        throw BadCast("Return value is not a null");
    }
    return nullptr;
}
#endif

/**
     * @ingroup wrenbind17
     * @see ReturnValue
     * @brief An alias of ReturnValue class
     */
using Any = ReturnValue;

#ifndef DOXYGEN_SHOULD_SKIP_THIS
template <>
inline Any detail::getSlot(WrenVM* vm, const int idx) {
    const auto type = wrenGetSlotType(vm, 0);
    if (type == WREN_TYPE_NULL) {
        return Any();
    }
    return Any(type, Handle(getSharedVm(vm), wrenGetSlotHandle(vm, idx)));
}
#endif
} // namespace wrenbind17
