#pragma once

#include <memory>
#include <string>
#include <wren.hpp>

#include "object.hpp"

namespace wrenbind17 {
void getClassType(WrenVM* vm, std::string& module, std::string& name, size_t hash);
detail::ForeignPtrConvertor* getClassCast(WrenVM* vm, size_t hash, size_t other);

#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
// ============================================================================================================
//                                       CHECK SLOTS FOR TYPE
// ============================================================================================================
inline const char* wrenSlotTypeToStr(const WrenType type) {
    switch (type) {
    case WREN_TYPE_BOOL:
        return "bool";
    case WREN_TYPE_FOREIGN:
        return "instance";
    case WREN_TYPE_LIST:
        return "list";
    case WREN_TYPE_NULL:
        return "null";
    case WREN_TYPE_NUM:
        return "number";
    case WREN_TYPE_STRING:
        return "string";
    case WREN_TYPE_UNKNOWN:
    default:
        return "unknown";
    }
}

template <typename T>
inline bool is(WrenVM* vm, const int idx) {
    const auto type = wrenGetSlotType(vm, idx);
    if (type != WrenType::WREN_TYPE_FOREIGN) return false;

    auto slot = wrenGetSlotForeign(vm, idx);
    const auto foreign = reinterpret_cast<Foreign*>(slot);
    return foreign->hash() == typeid(T).hash_code();
}

template <>
inline bool is<bool>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_BOOL;
}

template <>
inline bool is<std::string>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_STRING;
}

template <>
inline bool is<std::nullptr_t>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NULL;
}

template <>
inline bool is<float>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <>
inline bool is<double>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <>
inline bool is<int>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <>
inline bool is<int8_t>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <>
inline bool is<char>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <>
inline bool is<unsigned char>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <>
inline bool is<short>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <>
inline bool is<unsigned short>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <>
inline bool is<unsigned>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <>
inline bool is<long>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <>
inline bool is<long long>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <>
inline bool is<unsigned long>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <>
inline bool is<unsigned long long>(WrenVM* vm, const int idx) {
    return wrenGetSlotType(vm, idx) == WrenType::WREN_TYPE_NUM;
}

template <typename T>
struct CheckSlot {
    static bool f(WrenVM* vm, const int idx) { return is<T>(vm, idx); }
};

template <typename T>
struct CheckSlot<T&> {
    static bool f(WrenVM* vm, const int idx) { return is<T>(vm, idx); }
};

template <typename T>
struct CheckSlot<const T&> {
    static bool f(WrenVM* vm, const int idx) { return is<T>(vm, idx); }
};

template <typename T>
struct CheckSlot<T*> {
    static bool f(WrenVM* vm, const int idx) { return is<T>(vm, idx); }
};

template <typename T>
struct CheckSlot<const T*> {
    static bool f(WrenVM* vm, const int idx) { return is<T>(vm, idx); }
};

template <typename T>
struct CheckSlot<std::shared_ptr<T>> {
    static bool f(WrenVM* vm, const int idx) { return is<T>(vm, idx); }
};

template <typename T>
struct CheckSlot<const std::shared_ptr<T>&> {
    static bool f(WrenVM* vm, const int idx) { return is<T>(vm, idx); }
};

// ============================================================================================================
//                                       BASIC TYPES
// ============================================================================================================
template <WrenType Type>
inline void validate(WrenVM* vm, int idx) {
    const auto t = wrenGetSlotType(vm, idx);
    if (t != Type)
        throw BadCast("Bad cast when getting value from Wren got " + std::string(wrenSlotTypeToStr(t)) + " expected " +
                      std::string(wrenSlotTypeToStr(Type)));
}

template <typename T>
std::shared_ptr<T> getSlotForeign(WrenVM* vm, void* slot) {
    using Type = typename std::remove_const<typename std::remove_pointer<T>::type>::type;
    using ForeignTypeConvertor = ForeignSharedPtrConvertor<Type>;

    const auto foreign = reinterpret_cast<Foreign*>(slot);
    if (foreign->hash() != typeid(Type).hash_code()) {
        try {
            auto base = getClassCast(vm, foreign->hash(), typeid(Type).hash_code());
            auto derived = reinterpret_cast<ForeignTypeConvertor*>(base);
            if (!derived) {
                throw BadCast("Bad cast the value cannot be upcast to the expected type");
            }
            return derived->cast(foreign);
        } catch (std::out_of_range& e) {
            (void)e;
            throw BadCast("Bad cast the value is not the expected type");
        }
    }

    auto ptr = reinterpret_cast<ForeignObject<Type>*>(foreign);
    return ptr->shared();
}

template <typename T>
const std::shared_ptr<T> getSlotForeign(WrenVM* vm, const int idx) {
    validate<WrenType::WREN_TYPE_FOREIGN>(vm, idx);
    return getSlotForeign<T>(vm, wrenGetSlotForeign(vm, idx));
}

template <typename T>
T getSlot(WrenVM* vm, int idx) {
    static_assert(!std::is_same<std::string, T>(), "type can't be std::string");
    static_assert(!is_shared_ptr<T>::value, "type can't be shared_ptr<T>");
    validate<WrenType::WREN_TYPE_FOREIGN>(vm, idx);
    return *getSlotForeign<typename std::remove_reference<T>::type>(vm, idx).get();
}

template <typename T>
struct PopHelper {
    static inline T f(WrenVM* vm, int idx) { return getSlot<T>(vm, idx); }
};

template <typename T>
struct PopHelper<T*> {
    static inline T* f(WrenVM* vm, int idx) {
        const auto type = wrenGetSlotType(vm, idx);
        if (type == WrenType::WREN_TYPE_NULL)
            return nullptr;
        else if (type != WrenType::WREN_TYPE_FOREIGN)
            throw BadCast("Bad cast when getting value from Wren");
        return getSlotForeign<typename std::remove_const<T>::type>(vm, idx).get();
    }
};

template <typename T>
struct PopHelper<const T&> {
    static inline const T& f(WrenVM* vm, int idx) {
        static_assert(!std::is_same<std::string, T>(), "type can't be std::string");
        static_assert(!std::is_same<std::nullptr_t, T>(), "type can't be std::nullptr_t");
        static_assert(!is_shared_ptr<T>::value, "type can't be shared_ptr<T>");
        return *getSlotForeign<T>(vm, idx).get();
    }
};

template <typename T>
struct PopHelper<std::shared_ptr<T>> {
    static inline std::shared_ptr<T> f(WrenVM* vm, int idx) {
        const auto type = wrenGetSlotType(vm, idx);
        if (type == WrenType::WREN_TYPE_NULL)
            return nullptr;
        else if (type != WrenType::WREN_TYPE_FOREIGN)
            throw BadCast("Bad cast when getting value from Wren");
        return getSlotForeign<T>(vm, idx);
    }
};

template <typename T>
struct PopHelper<const std::shared_ptr<T>&> {
    static inline std::shared_ptr<T> f(WrenVM* vm, int idx) {
        const auto type = wrenGetSlotType(vm, idx);
        if (type == WrenType::WREN_TYPE_NULL)
            return nullptr;
        else if (type != WrenType::WREN_TYPE_FOREIGN)
            throw BadCast("Bad cast when getting value from Wren");
        return getSlotForeign<T>(vm, idx);
    }
};

template <>
inline Handle getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_UNKNOWN>(vm, idx);
    return Handle(getSharedVm(vm), wrenGetSlotHandle(vm, idx));
}

template <>
inline std::string getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_STRING>(vm, idx);
    return std::string(wrenGetSlotString(vm, idx));
}

template <>
inline std::nullptr_t getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NULL>(vm, idx);
    return nullptr;
}

template <>
inline bool getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_BOOL>(vm, idx);
    return wrenGetSlotBool(vm, idx);
}

template <>
inline int8_t getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<int8_t>(wrenGetSlotDouble(vm, idx));
}

template <>
inline char getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<char>(wrenGetSlotDouble(vm, idx));
}

template <>
inline int getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<int>(wrenGetSlotDouble(vm, idx));
}

template <>
inline short getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<short>(wrenGetSlotDouble(vm, idx));
}

template <>
inline long getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<long>(wrenGetSlotDouble(vm, idx));
}

template <>
inline unsigned char getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<unsigned char>(wrenGetSlotDouble(vm, idx));
}

template <>
inline unsigned long getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<unsigned long>(wrenGetSlotDouble(vm, idx));
}

template <>
inline unsigned short getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<unsigned short>(wrenGetSlotDouble(vm, idx));
}

template <>
inline unsigned getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<unsigned>(wrenGetSlotDouble(vm, idx));
}

template <>
inline long long getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<long long>(wrenGetSlotDouble(vm, idx));
}

template <>
inline unsigned long long getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<unsigned long long>(wrenGetSlotDouble(vm, idx));
}

template <>
inline float getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<float>(wrenGetSlotDouble(vm, idx));
}

template <>
inline double getSlot(WrenVM* vm, int idx) {
    validate<WrenType::WREN_TYPE_NUM>(vm, idx);
    return static_cast<double>(wrenGetSlotDouble(vm, idx));
}

#define WRENBIND17_POP_HELPER(Type)                                                  \
    template <>                                                                      \
    struct PopHelper<const Type&> {                                                  \
        static inline Type f(WrenVM* vm, int idx) { return getSlot<Type>(vm, idx); } \
    };

WRENBIND17_POP_HELPER(std::string)
WRENBIND17_POP_HELPER(std::nullptr_t)
WRENBIND17_POP_HELPER(bool)
WRENBIND17_POP_HELPER(int8_t)
WRENBIND17_POP_HELPER(char)
WRENBIND17_POP_HELPER(int)
WRENBIND17_POP_HELPER(short)
WRENBIND17_POP_HELPER(long)
WRENBIND17_POP_HELPER(unsigned long)
WRENBIND17_POP_HELPER(unsigned)
WRENBIND17_POP_HELPER(long long)
WRENBIND17_POP_HELPER(unsigned long long)
WRENBIND17_POP_HELPER(unsigned short)
WRENBIND17_POP_HELPER(unsigned char)
WRENBIND17_POP_HELPER(float)
WRENBIND17_POP_HELPER(double)
} // namespace detail
#endif
} // namespace wrenbind17
