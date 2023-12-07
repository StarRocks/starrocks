#pragma once

#include "method.hpp"
#include "pop.hpp"
#include "push.hpp"

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
/**
     * @ingroup wrenbind17
     * @brief Holds native Wren map
     */
class Map {
public:
    Map() {}
    Map(const std::shared_ptr<Handle>& handle) : handle(handle) {}
    ~Map() { reset(); }

    Handle& getHandle() { return *handle; }

    const Handle& getHandle() const { return *handle; }

    operator bool() const { return handle.operator bool(); }

    void reset() { handle.reset(); }

    /*!
         * @brief Checks if a key exists in this map
         * @throws RuntimeError if this is an invalid map or the Wren VM has terminated
         * @warning If using strings, make sure that you use std::string because
         * raw C-strings are not allowed.
         * @note This function accepts any type you want. Integers, strings, booleans,
         * does not matter as long as Wren map supports that key type.
         */
    template <typename Key>
    bool contains(const Key& key) const {
        if (const auto ptr = handle->getVmWeak().lock()) {
            wrenEnsureSlots(ptr.get(), 2);
            wrenSetSlotHandle(ptr.get(), 0, handle->getHandle());
            detail::PushHelper<Key>::f(ptr.get(), 1, key);
            return wrenGetMapContainsKey(ptr.get(), 0, 1);
        } else {
            throw RuntimeError("Invalid handle");
        }
    }

    /*!
         * @brief Returns a value specified by T from the map by a key
         * @throws NotFound if the key does not exist in the map
         * @throws RuntimeError if this is an invalid map or the Wren VM has terminated
         * @warning If using strings, make sure that you use std::string because
         * raw C-strings are not allowed.
         * @note This function accepts any type you want. Integers, strings, booleans,
         * does not matter as long as Wren map supports that key type.
         */
    template <typename T, typename Key>
    T get(const Key& key) const {
        if (const auto ptr = handle->getVmWeak().lock()) {
            wrenEnsureSlots(ptr.get(), 3);
            wrenSetSlotHandle(ptr.get(), 0, handle->getHandle());
            detail::PushHelper<Key>::f(ptr.get(), 1, key);
            if (!wrenGetMapContainsKey(ptr.get(), 0, 1)) {
                throw NotFound();
            }
            wrenGetMapValue(ptr.get(), 0, 1, 2);
            return detail::PopHelper<T>::f(ptr.get(), 2);
        } else {
            throw RuntimeError("Invalid handle");
        }
    }

    /*!
         * @brief Erases a key from the map
         * @throws NotFound if the key does not exist in the map
         * @throws RuntimeError if this is an invalid map or the Wren VM has terminated
         * @returns true if the key has been erased, otherwise false
         * @warning If using strings, make sure that you use std::string because
         * raw C-strings are not allowed.
         * @note This function accepts any type you want. Integers, strings, booleans,
         * does not matter as long as Wren map supports that key type.
         */
    template <typename Key>
    bool erase(const Key& key) const {
        if (const auto ptr = handle->getVmWeak().lock()) {
            wrenEnsureSlots(ptr.get(), 3);
            wrenSetSlotHandle(ptr.get(), 0, handle->getHandle());
            detail::PushHelper<Key>::f(ptr.get(), 1, key);
            wrenRemoveMapValue(ptr.get(), 0, 1, 2);
            return !detail::is<std::nullptr_t>(ptr.get(), 2);
        } else {
            throw RuntimeError("Invalid handle");
        }
    }

    /*!
         * @brief Returns the size of the map
         * @throws RuntimeError if this is an invalid map or the Wren VM has terminated
         */
    size_t count() const {
        if (const auto ptr = handle->getVmWeak().lock()) {
            wrenEnsureSlots(ptr.get(), 1);
            wrenSetSlotHandle(ptr.get(), 0, handle->getHandle());
            return wrenGetMapCount(ptr.get(), 0);
        } else {
            throw RuntimeError("Invalid handle");
        }
    }

private:
    std::shared_ptr<Handle> handle;
};

template <>
inline Map detail::getSlot<Map>(WrenVM* vm, const int idx) {
    validate<WrenType::WREN_TYPE_MAP>(vm, idx);
    return Map(std::make_shared<Handle>(getSharedVm(vm), wrenGetSlotHandle(vm, idx)));
}

template <>
inline bool detail::is<Map>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WREN_TYPE_MAP;
}

template <>
inline void detail::PushHelper<Map>::f(WrenVM* vm, int idx, const Map& value) {
    wrenSetSlotHandle(value.getHandle().getVm(), idx, value.getHandle().getHandle());
}

template <>
inline void detail::PushHelper<Map>::f(WrenVM* vm, int idx, Map&& value) {
    wrenSetSlotHandle(value.getHandle().getVm(), idx, value.getHandle().getHandle());
}

template <>
inline void detail::PushHelper<const Map>::f(WrenVM* vm, int idx, const Map value) {
    wrenSetSlotHandle(value.getHandle().getVm(), idx, value.getHandle().getHandle());
}

template <>
inline void detail::PushHelper<const Map&>::f(WrenVM* vm, int idx, const Map& value) {
    wrenSetSlotHandle(value.getHandle().getVm(), idx, value.getHandle().getHandle());
}

template <>
inline void detail::PushHelper<Map&>::f(WrenVM* vm, int idx, Map& value) {
    wrenSetSlotHandle(value.getHandle().getVm(), idx, value.getHandle().getHandle());
}
} // namespace wrenbind17
