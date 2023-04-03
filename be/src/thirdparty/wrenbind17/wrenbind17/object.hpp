#pragma once

#include <cstdlib>
#include <memory>
#include <string>
#include <typeinfo>
#include <variant>
#include <wren.hpp>

#include "exception.hpp"
#include "handle.hpp"

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
#ifndef DOXYGEN_SHOULD_SKIP_THIS
std::string getLastError(WrenVM* vm);

inline void exceptionHandler(WrenVM* vm, const std::exception_ptr& eptr) {
    try {
        if (eptr) {
            std::rethrow_exception(eptr);
        } else {
            wrenEnsureSlots(vm, 1);
            wrenSetSlotString(vm, 0, "Unknown error");
            wrenAbortFiber(vm, 0);
        }
    } catch (std::exception& e) {
        wrenEnsureSlots(vm, 1);
        wrenSetSlotString(vm, 0, e.what());
        wrenAbortFiber(vm, 0);
    }
}

template <class T>
struct is_shared_ptr : std::false_type {};
template <class T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type {};

namespace detail {
class Foreign {
public:
    Foreign() = default;
    virtual ~Foreign() = 0;
    virtual void* get() const = 0;
    virtual size_t hash() const = 0;
};

inline Foreign::~Foreign() {}

template <typename T>
class ForeignObject : public Foreign {
public:
    ForeignObject() {}

    ForeignObject(std::shared_ptr<T> ptr) : ptr(std::move(ptr)) {}

    virtual ~ForeignObject() = default;

    void* get() const override { return ptr.get(); }

    size_t hash() const override { return typeid(T).hash_code(); }

    const std::shared_ptr<T>& shared() const { return ptr; }

    std::shared_ptr<T> ptr;
};

class ForeignPtrConvertor {
public:
    ForeignPtrConvertor() = default;
    virtual ~ForeignPtrConvertor() = default;
};

template <typename T>
class ForeignSharedPtrConvertor : public ForeignPtrConvertor {
public:
    ForeignSharedPtrConvertor() = default;
    virtual ~ForeignSharedPtrConvertor() = default;

    virtual std::shared_ptr<T> cast(Foreign* foreign) const = 0;
};

template <typename From, typename To>
class ForeignObjectSharedPtrConvertor : public ForeignSharedPtrConvertor<To> {
public:
    ForeignObjectSharedPtrConvertor() = default;
    virtual ~ForeignObjectSharedPtrConvertor() = default;

    inline std::shared_ptr<To> cast(Foreign* foreign) const override {
        if (!foreign) throw Exception("Cannot upcast foreign pointer is null and this should not happen");
        auto* ptr = dynamic_cast<ForeignObject<From>*>(foreign);
        if (!ptr) throw BadCast("Bad cast while upcasting to a base type");
        return std::dynamic_pointer_cast<To>(ptr->shared());
    }
};

template <class T>
struct is_shared_ptr : std::false_type {};
template <class T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type {};
} // namespace detail
#endif
} // namespace wrenbind17
