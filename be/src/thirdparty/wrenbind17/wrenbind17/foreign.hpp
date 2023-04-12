#pragma once

#include <iostream>
#include <ostream>
#include <sstream>
#include <unordered_map>
#include <wren.hpp>

#include "allocator.hpp"
#include "caller.hpp"

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
/**
     * @ingroup wrenbind17
     */
enum ForeignMethodOperator {
    OPERATOR_GET_INDEX,
    OPERATOR_SET_INDEX,
    OPERATOR_SUB,
    OPERATOR_ADD,
    OPERATOR_MUL,
    OPERATOR_DIV,
    OPERATOR_NEG,
    OPERATOR_MOD,
    OPERATOR_EQUAL,
    OPERATOR_NOT_EQUAL,
    OPERATOR_GT,
    OPERATOR_LT,
    OPERATOR_GT_EQUAL,
    OPERATOR_LT_EQUAL,
    OPERATOR_SHIFT_LEFT,
    OPERATOR_SHIFT_RIGHT,
    OPERATOR_AND,
    OPERATOR_XOR,
    OPERATOR_OR
};
/**
     * @ingroup wrenbind17
     * @brief Holds information about a foreign function of a foreign class
     */
class ForeignMethod {
public:
    ForeignMethod(std::string name, WrenForeignMethodFn method, const bool isStatic)
            : name(std::move(name)), method(method), isStatic(isStatic) {}

    virtual ~ForeignMethod() = default;

    // If you are getting "referencing deleted function" error which is this one below,
    // then you are trying to make a copy of ForeignKlass during vm.klass<Foo>("Foo");
    ForeignMethod(const ForeignMethod& other) = delete;

    virtual void generate(std::ostream& os) const = 0;

    /*!
         * @brief Returns the name of the method
         */
    const std::string& getName() const { return name; }

    /*!
         * @brief Returns the raw pointer of this method
         */
    WrenForeignMethodFn getMethod() const { return method; }

    /*!
         * @brief Returns true if this method is marked as static
         */
    bool getStatic() const { return isStatic; }

protected:
    std::string name;
    WrenForeignMethodFn method;
    bool isStatic;
};

/**
     * @ingroup wrenbind17
     * @brief Holds information about a foreign property of a foreign class
     */
class ForeignProp {
public:
    ForeignProp(std::string name, WrenForeignMethodFn getter, WrenForeignMethodFn setter, const bool isStatic)
            : name(std::move(name)), getter(getter), setter(setter), isStatic(isStatic) {}

    virtual ~ForeignProp() = default;

    // If you are getting "referencing deleted function" error which is this one below,
    // then you are trying to make a copy of ForeignKlass during vm.klass<Foo>("Foo");
    ForeignProp(const ForeignProp& other) = delete;

    void generate(std::ostream& os) const {
        if (getter) os << "    foreign " << name << "\n";
        if (setter) os << "    foreign " << name << "=(rhs)\n";
    }

    /*!
         * @brief Returns the name of this property
         */
    const std::string& getName() const { return name; }

    /*!
         * @brief Returns the pointer to the raw function for settings this property
         */
    WrenForeignMethodFn getSetter() { return setter; }

    /*!
         * @brief Returns the pointer to the raw function for getting this property
         */
    WrenForeignMethodFn getGetter() { return getter; }

    /*!
         * @brief Returns true if this property is static
         */
    bool getStatic() const { return isStatic; }

protected:
    std::string name;
    WrenForeignMethodFn getter;
    WrenForeignMethodFn setter;
    bool isStatic;
};

/**
     * @ingroup wrenbind17
     * @brief A foreign class
     */
class ForeignKlass {
public:
    ForeignKlass(std::string name) : name(std::move(name)), allocators{nullptr, nullptr} {}

    virtual ~ForeignKlass() = default;

    // If you are getting "referencing deleted function" error which is this one below,
    // then you are trying to make a copy of this class.
    ForeignKlass(const ForeignKlass& other) = delete;

    virtual void generate(std::ostream& os) const = 0;

    /*!
         * @brief Looks up a foreign function that belongs to this class
         */
    ForeignMethod& findFunc(const std::string& name, const bool isStatic) {
        const auto it = methods.find(name);
        if (it == methods.end()) throw NotFound();
        if (it->second->getStatic() != isStatic) throw NotFound();
        return *it->second;
    }

    /*!
         * @brief Looks up a foreign property that belongs to this class
         */
    ForeignProp& findProp(const std::string& name, const bool isStatic) {
        const auto it = props.find(name);
        if (it == props.end()) throw NotFound();
        if (it->second->getStatic() != isStatic) throw NotFound();
        return *it->second;
    }

    /*!
         * @brief Finds a function based on the signature
         */
    WrenForeignMethodFn findSignature(const std::string& signature, const bool isStatic) {
        switch (signature[0]) {
        case '[':
        case '-':
        case '+':
        case '/':
        case '*':
        case '=':
        case '!':
        case '%':
        case '<':
        case '>':
        case '&':
        case '^':
        case '|': {
            // Operators
            return findFunc(signature, isStatic).getMethod();
        }
        default: {
            if (signature.find('(') != std::string::npos) {
                // Check if setter
                if (signature.find("=(_)") != std::string::npos) {
                    return findProp(signature.substr(0, signature.find_first_of('=')), isStatic).getSetter();
                } else {
                    // Must be a method
                    return findFunc(signature, isStatic).getMethod();
                }

            } else {
                return findProp(signature, isStatic).getGetter();
            }
        }
        }
    }

    /*!
         * @brief Returns the name of this foreign class
         */
    const std::string& getName() const { return name; }

    /*!
         * @brief Returns a struct with pointers to the allocator and deallocator
         */
    WrenForeignClassMethods& getAllocators() { return allocators; }

protected:
    std::string name;
    std::string ctorDef;
    std::unordered_map<std::string, std::unique_ptr<ForeignMethod>> methods;
    std::unordered_map<std::string, std::unique_ptr<ForeignProp>> props;
    WrenForeignClassMethods allocators;
};

/**
     * @ingroup wrenbind17
     * @brief Type specific implementation of foreign method
     */
template <typename... Args>
class ForeignMethodImpl : public ForeignMethod {
public:
    ForeignMethodImpl(std::string name, std::string signature, WrenForeignMethodFn fn, const bool isStatic)
            : ForeignMethod(std::move(name), fn, isStatic), signature(std::move(signature)) {}
    ~ForeignMethodImpl() = default;

    void generate(std::ostream& os) const override {
        os << "    foreign " << (isStatic ? "static " : "") << signature << "\n";
    }

    static std::string generateSignature(const std::string& name) {
        std::stringstream os;
        constexpr auto n = sizeof...(Args);
        os << name << "(";
        for (size_t i = 0; i < n; i++) {
            if (i == 0)
                os << "arg0";
            else
                os << ", arg" << i;
        }
        os << ")";
        return os.str();
    }

    static std::string generateSignature(const ForeignMethodOperator name) {
        switch (name) {
        case OPERATOR_GET_INDEX:
            return "[arg]";
        case OPERATOR_SET_INDEX:
            return "[arg]=(rhs)";
        case OPERATOR_ADD:
            return "+(rhs)";
        case OPERATOR_SUB:
            return "-(rhs)";
        case OPERATOR_DIV:
            return "/(rhs)";
        case OPERATOR_MUL:
            return "*(rhs)";
        case OPERATOR_MOD:
            return "%(rhs)";
        case OPERATOR_EQUAL:
            return "==(rhs)";
        case OPERATOR_NOT_EQUAL:
            return "!=(rhs)";
        case OPERATOR_NEG:
            return "-";
        case OPERATOR_GT:
            return ">(rhs)";
        case OPERATOR_LT:
            return "<(rhs)";
        case OPERATOR_GT_EQUAL:
            return ">=(rhs)";
        case OPERATOR_LT_EQUAL:
            return "<=(rhs)";
        case OPERATOR_SHIFT_LEFT:
            return "<<(rhs)";
        case OPERATOR_SHIFT_RIGHT:
            return ">>(rhs)";
        case OPERATOR_AND:
            return "&(rhs)";
        case OPERATOR_XOR:
            return "^(rhs)";
        case OPERATOR_OR:
            return "|(rhs)";
        default:
            throw Exception("Operator not supported");
        }
    }

    static std::string generateName(const ForeignMethodOperator name) {
        switch (name) {
        case OPERATOR_GET_INDEX:
            return "[_]";
        case OPERATOR_SET_INDEX:
            return "[_]=(_)";
        case OPERATOR_ADD:
            return "+(_)";
        case OPERATOR_SUB:
            return "-(_)";
        case OPERATOR_DIV:
            return "/(_)";
        case OPERATOR_MUL:
            return "*(_)";
        case OPERATOR_MOD:
            return "%(_)";
        case OPERATOR_EQUAL:
            return "==(_)";
        case OPERATOR_NOT_EQUAL:
            return "!=(_)";
        case OPERATOR_NEG:
            return "-";
        case OPERATOR_GT:
            return ">(_)";
        case OPERATOR_LT:
            return "<(_)";
        case OPERATOR_GT_EQUAL:
            return ">=(_)";
        case OPERATOR_LT_EQUAL:
            return "<=(_)";
        case OPERATOR_SHIFT_LEFT:
            return "<<(_)";
        case OPERATOR_SHIFT_RIGHT:
            return ">>(_)";
        case OPERATOR_AND:
            return "&(_)";
        case OPERATOR_XOR:
            return "^(_)";
        case OPERATOR_OR:
            return "|(_)";
        default:
            throw Exception("Operator not supported");
        }
    }

private:
    std::string signature;
};

/**
     * @ingroup wrenbind17
     */
template <typename T, typename V>
class ForeignPropImpl : public ForeignProp {
public:
    ForeignPropImpl(std::string name, WrenForeignMethodFn getter, WrenForeignMethodFn setter, const bool isStatic)
            : ForeignProp(std::move(name), getter, setter, isStatic) {}
    ~ForeignPropImpl() = default;
};

#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
template <typename... Args>
inline std::string generateNameArgs() {
    constexpr auto n = sizeof...(Args);
    std::stringstream ss;
    ss << "(";
    for (size_t i = 0; i < n; i++) {
        ss << "_";
        if (i != n - 1) {
            ss << ",";
        }
    }
    ss << ")";
    return ss.str();
}

template <typename Signature, Signature signature>
struct ForeignFunctionDetails;

template <typename R, typename... Args, R (*Fn)(Args...)>
struct ForeignFunctionDetails<R (*)(Args...), Fn> {
    typedef ForeignMethodImpl<Args...> ForeignMethodImplType;

    static std::unique_ptr<ForeignMethodImplType> make(std::string name) {
        auto signature = ForeignMethodImplType::generateSignature(name);
        auto p = detail::ForeignFunctionCaller<R, Args...>::template call<Fn>;
        name = name + detail::generateNameArgs<Args...>();
        return std::make_unique<ForeignMethodImplType>(std::move(name), std::move(signature), p, true);
    }
};

template <typename M>
struct GetPointerType {
    template <typename C, typename T>
    static T getType(T C::*v);

    typedef decltype(getType(static_cast<M>(nullptr))) type;
};

template <typename Var, Var var>
struct GetVarTraits;

template <typename C, typename T, T C::*Var>
struct GetVarTraits<T C::*, Var> {
    using klass = C;
};
} // namespace detail
#endif

/**
     * @ingroup wrenbind17
     * @brtief Type specific implementation of foreign class
     */
template <typename T>
class ForeignKlassImpl : public ForeignKlass {
public:
    ForeignKlassImpl(std::string name) : ForeignKlass(std::move(name)) {
        allocators.allocate = nullptr;
        allocators.finalize = nullptr;
    }

    ~ForeignKlassImpl() = default;

    // If you are getting "referencing deleted function" error which is this one below,
    // then you are trying to make a copy of this class.
    // Make sure you are not creating a copy while registering your custom C++ class as:
    // auto& cls = vm.module("mymodule").klass<Foo>("Foo");
    // Notice the "auto&"
    ForeignKlassImpl(const ForeignKlassImpl<T>& other) = delete;

    /*!
         * @brief Generate Wren code for this class
         */
    void generate(std::ostream& os) const override {
        os << "foreign class " << name << " {\n";
        if (!ctorDef.empty()) {
            os << "    " << ctorDef;
        }
        for (const auto& pair : methods) {
            pair.second->generate(os);
        }
        for (const auto& pair : props) {
            pair.second->generate(os);
        }
        os << "}\n\n";
    }

    /*!
         * @brief Add a constructor to this class
         */
    template <typename... Args>
    void ctor(const std::string& name = "new") {
        allocators.allocate = &detail::ForeignKlassAllocator<T, Args...>::allocate;
        allocators.finalize = &detail::ForeignKlassAllocator<T, Args...>::finalize;
        std::stringstream ss;
        ss << "construct " << name << " (";
        constexpr auto n = sizeof...(Args);
        for (size_t i = 0; i < n; i++) {
            if (i == 0)
                ss << "arg0";
            else
                ss << ", arg" << i;
        }
        ss << ") {}\n\n";
        ctorDef = ss.str();
    }

#ifndef DOXYGEN_SHOULD_SKIP_THIS
    template <typename Signature, Signature signature>
    struct ForeignMethodDetails;

    template <typename R, typename C, typename... Args, R (C::*Fn)(Args...)>
    struct ForeignMethodDetails<R (C::*)(Args...), Fn> {
        static_assert(std::is_base_of<C, T>::value, "The method belong to its own class or a base class");

        typedef ForeignMethodImpl<Args...> ForeignMethodImplType;

        static std::unique_ptr<ForeignMethodImplType> make(std::string name) {
            auto signature = ForeignMethodImplType::generateSignature(name);
            auto p = detail::ForeignMethodCaller<R, C, Args...>::template call<Fn>;
            name = name + detail::generateNameArgs<Args...>();
            return std::make_unique<ForeignMethodImplType>(std::move(name), std::move(signature), p, false);
        }

        static std::unique_ptr<ForeignMethodImplType> make(const ForeignMethodOperator op) {
            auto signature = ForeignMethodImplType::generateSignature(op);
            auto name = ForeignMethodImplType::generateName(op);
            auto p = detail::ForeignMethodCaller<R, C, Args...>::template call<Fn>;
            return std::make_unique<ForeignMethodImplType>(std::move(name), std::move(signature), p, false);
        }
    };

    template <typename R, typename C, typename... Args, R (C::*Fn)(Args...) const>
    struct ForeignMethodDetails<R (C::*)(Args...) const, Fn> {
        static_assert(std::is_base_of<C, T>::value, "The method belong to its own class or a base class");

        typedef ForeignMethodImpl<Args...> ForeignMethodImplType;

        static std::unique_ptr<ForeignMethodImplType> make(std::string name) {
            auto signature = ForeignMethodImplType::generateSignature(name);
            auto p = detail::ForeignMethodCaller<R, C, Args...>::template call<Fn>;
            name = name + detail::generateNameArgs<Args...>();
            return std::make_unique<ForeignMethodImplType>(std::move(name), std::move(signature), p, false);
        }

        static std::unique_ptr<ForeignMethodImplType> make(const ForeignMethodOperator op) {
            auto signature = ForeignMethodImplType::generateSignature(op);
            auto name = ForeignMethodImplType::generateName(op);
            auto p = detail::ForeignMethodCaller<R, C, Args...>::template call<Fn>;
            return std::make_unique<ForeignMethodImplType>(std::move(name), std::move(signature), p, false);
        }
    };

    template <typename Signature, Signature signature>
    struct ForeignMethodExtDetails;

    template <typename R, typename... Args, R (*Fn)(T&, Args...)>
    struct ForeignMethodExtDetails<R (*)(T&, Args...), Fn> {
        typedef ForeignMethodImpl<Args...> ForeignMethodImplType;

        static std::unique_ptr<ForeignMethodImplType> make(std::string name) {
            auto signature = ForeignMethodImplType::generateSignature(name);
            auto p = detail::ForeignMethodExtCaller<R, T, Args...>::template call<Fn>;
            name = name + detail::generateNameArgs<Args...>();
            return std::make_unique<ForeignMethodImplType>(std::move(name), std::move(signature), p, false);
        }

        static std::unique_ptr<ForeignMethodImplType> make(const ForeignMethodOperator op) {
            auto signature = ForeignMethodImplType::generateSignature(op);
            auto name = ForeignMethodImplType::generateName(op);
            auto p = detail::ForeignMethodExtCaller<R, T, Args...>::template call<Fn>;
            return std::make_unique<ForeignMethodImplType>(std::move(name), std::move(signature), p, false);
        }
    };

    template <typename V, typename C, V C::*Ptr>
    struct ForeignVarDetails {
        static_assert(std::is_base_of<C, T>::value, "The variable belong to its own class or a base class");

        static std::unique_ptr<ForeignProp> make(std::string name, const bool readonly) {
            auto s = readonly ? nullptr : detail::ForeignPropCaller<C, V, Ptr>::setter;
            auto g = detail::ForeignPropCaller<C, V, Ptr>::getter;
            return std::make_unique<ForeignProp>(std::move(name), g, s, false);
        }
    };

    template <typename V, typename C, V C::*Ptr>
    struct ForeignVarReadonlyDetails {
        static_assert(std::is_base_of<C, T>::value, "The variable belong to its own class or a base class");

        static std::unique_ptr<ForeignProp> make(std::string name, const bool readonly) {
            auto g = detail::ForeignPropCaller<C, V, Ptr>::getter;
            return std::make_unique<ForeignProp>(std::move(name), g, nullptr, false);
        }
    };

    template <typename Signature, Signature signature>
    struct ForeignSetterDetails;

    template <typename V, typename C, void (C::*Fn)(V)>
    struct ForeignSetterDetails<void (C::*)(V), Fn> {
        static_assert(std::is_base_of<C, T>::value, "The setter must belong to its own class or a base class");

        static WrenForeignMethodFn method() { return detail::ForeignMethodCaller<void, C, V>::template call<Fn>; }
    };

    template <typename Signature, Signature signature>
    struct ForeignSetterExtDetails;

    template <typename V, void (*Fn)(T&, V)>
    struct ForeignSetterExtDetails<void (*)(T&, V), Fn> {
        static WrenForeignMethodFn method() { return detail::ForeignMethodExtCaller<void, T, V>::template call<Fn>; }
    };

    template <typename Signature, Signature signature>
    struct ForeignGetterDetails;

    template <typename R, typename C, R (C::*Fn)()>
    struct ForeignGetterDetails<R (C::*)(), Fn> {
        static_assert(std::is_base_of<C, T>::value, "The getter must belong to its own class or a base class");

        static WrenForeignMethodFn method() { return detail::ForeignMethodCaller<R, C>::template call<Fn>; }
    };

    template <typename R, typename C, R (C::*Fn)() const>
    struct ForeignGetterDetails<R (C::*)() const, Fn> {
        static_assert(std::is_base_of<C, T>::value, "The getter must belong to its own class or a base class");

        static WrenForeignMethodFn method() { return detail::ForeignMethodCaller<R, C>::template call<Fn>; }
    };

    template <typename Signature, Signature signature>
    struct ForeignGetterExtDetails;

    template <typename R, R (*Fn)(T&)>
    struct ForeignGetterExtDetails<R (*)(T&), Fn> {
        static WrenForeignMethodFn method() { return detail::ForeignMethodExtCaller<R, T>::template call<Fn>; }
    };
#endif

    /*!
         * @brief Add a member function to this class
         * @details The number of arguments and what type of arguments
         * this class needs is handled at compile time with metaprogramming.
         * When this C++ function you are adding is called from Wren, it will check
         * the types passed and will match the C++ signature. If the types do not match
         * an exception is thrown that can be handled by Wren as a fiber.
         *
         * Example:
         *
         * @code
         * class Foo {
         * public:
         *     Foo(const std::string& msg) {...}
         *     void bar() {...}
         *     int baz() const {...}
         * };
         *
         * int main() {
         *     ...
         *     wren::VM vm;
         *     auto& m = vm.module("mymodule");
         *
         *     // Add class "Foo"
         *     auto& cls = m.klass<Foo>("Foo");
         *
         *     // Define constructor (you can only specify one constructor)
         *     cls.ctor<const std::string&>();
         *
         *     // Add some methods
         *     cls.func<&Foo::bar>("bar");
         *     cls.func<&Foo::baz>("baz");
         * }
         * @endcode
         */
    template <auto Fn>
    void func(std::string name) {
        auto ptr = ForeignMethodDetails<decltype(Fn), Fn>::make(std::move(name));
        methods.insert(std::make_pair(ptr->getName(), std::move(ptr)));
    }

    /*!
         * @brief Add a member operator function to this class that exists outside of the class
         * @see ForeignMethodOperator
         * @details The number of arguments and what type of arguments
         * this class needs is handled at compile time with metaprogramming.
         * When this C++ function you are adding is called from Wren, it will check
         * the types passed and will match the C++ signature. If the types do not match
         * an exception is thrown that can be handled by Wren as a fiber.
         *
         * Example:
         *
         * @code
         * class Foo {
         * public:
         *     Foo(const std::string& msg) {...}
         *     Foo& operator+(const std::string& other) {...}
         * };
         *
         * int main() {
         *     ...
         *     wren::VM vm;
         *     auto& m = vm.module("mymodule");
         *
         *     // Add class "Foo"
         *     auto& cls = m.klass<Foo>("Foo");
         *
         *     // Define constructor (you can only specify one constructor)
         *     cls.ctor<const std::string&>();
         *
         *     // Add some methods
         *     cls.func<&Foo::operator+>(wren::ForeignMethodOperator::OPERATOR_ADD);
         * }
         * @endcode
         */
    template <auto Fn>
    void func(const ForeignMethodOperator name) {
        auto ptr = ForeignMethodDetails<decltype(Fn), Fn>::make(name);
        methods.insert(std::make_pair(ptr->getName(), std::move(ptr)));
    }

    /*!
         * @brief Add a member function via a static function
         * @details The number of arguments and what type of arguments
         * this class needs is handled at compile time with metaprogramming.
         * When this C++ function you are adding is called from Wren, it will check
         * the types passed and will match the C++ signature. If the types do not match
         * an exception is thrown that can be handled by Wren as a fiber.
         *
         * This function does not accept class methods, but instead it uses regular functions
         * that have first parameter as "this" which is a reference to the class you are
         * adding.
         *
         * Example:
         *
         * @code
         * class Foo {
         * public:
         *     Foo(const std::string& msg) {...}
         *
         *     std::string message;
         * };
         *
         * static std::string fooBaz(Foo& self, int a, int b) {
         *     return self.message;
         * }
         *
         * int main() {
         *     ...
         *     wren::VM vm;
         *     auto& m = vm.module("mymodule");
         *
         *     // Add class "Foo"
         *     auto& cls = m.klass<Foo>("Foo");
         *
         *     // Define constructor (you can only specify one constructor)
         *     cls.ctor<const std::string&>();
         *
         *     // Add some methods
         *     cls.funcExt<&fooBaz>("bar");
         * }
         * @endcode
         */
    template <auto Fn>
    void funcExt(std::string name) {
        auto ptr = ForeignMethodExtDetails<decltype(Fn), Fn>::make(std::move(name));
        methods.insert(std::make_pair(ptr->getName(), std::move(ptr)));
    }

    /*!
         * @brief Add a member operator via a static function that exists outside of the class
         * @see ForeignMethodOperator
         * @details Same as funcExt but instead it can accept an operator enumeration.
         */
    template <auto Fn>
    void funcExt(const ForeignMethodOperator name) {
        auto ptr = ForeignMethodExtDetails<decltype(Fn), Fn>::make(name);
        methods.insert(std::make_pair(ptr->getName(), std::move(ptr)));
    }

    /*!
         * @brief Add a static function to this class
         * @see func
         * @details The number of arguments and what type of arguments
         * this class needs is handled at compile time with metaprogramming.
         * When this C++ function you are adding is called from Wren, it will check
         * the types passed and will match the C++ signature. If the types do not match
         * an exception is thrown that can be handled by Wren as a fiber.
         *
         * This only works if the function you are adding is static.
         *
         * Example:
         *
         * @code
         * class Foo {
         * public:
         *     Foo(const std::string& msg) {...}
         *     static void bar() {...}
         *     static int baz() const {...}
         * };
         *
         * int main() {
         *     ...
         *     wren::VM vm;
         *     auto& m = vm.module("mymodule");
         *
         *     // Add class "Foo"
         *     auto& cls = m.klass<Foo>("Foo");
         *
         *     // Define constructor (you can only specify one constructor)
         *     cls.ctor<const std::string&>();
         *
         *     // Add some methods
         *     cls.funcStatic<&Foo::bar>("bar");
         *     cls.funcStatic<&Foo::baz>("baz");
         * }
         * @endcode
         */
    template <auto Fn>
    void funcStatic(std::string name) {
        auto ptr = detail::ForeignFunctionDetails<decltype(Fn), Fn>::make(std::move(name));
        methods.insert(std::make_pair(ptr->getName(), std::move(ptr)));
    }

    /*!
         * @brief Add a static function to this class that exists outside of the class
         * @see funcStatic
         * @details The number of arguments and what type of arguments
         * this class needs is handled at compile time with metaprogramming.
         * When this C++ function you are adding is called from Wren, it will check
         * the types passed and will match the C++ signature. If the types do not match
         * an exception is thrown that can be handled by Wren as a fiber.
         *
         * This only works if the function you are adding is static.
         *
         * Example:
         *
         * @code
         * class Foo {
         * public:
         *     Foo(const std::string& msg) {...}
         * };
         *
         * static void fooBar() { ... }
         * static void fooBaz() { ... }
         *
         * int main() {
         *     ...
         *     wren::VM vm;
         *     auto& m = vm.module("mymodule");
         *
         *     // Add class "Foo"
         *     auto& cls = m.klass<Foo>("Foo");
         *
         *     // Define constructor (you can only specify one constructor)
         *     cls.ctor<const std::string&>();
         *
         *     // Add some methods
         *     cls.funcStatic<&fooBar>("bar");
         *     cls.funcStatic<&fooBaz>("baz");
         * }
         * @endcode
         */
    template <auto Fn>
    void funcStaticExt(std::string name) {
        // This is exactly the same as funcStatic because there is
        // no difference for "static void Foo::foo(){}" and "void foo(){}"!
        auto ptr = detail::ForeignFunctionDetails<decltype(Fn), Fn>::make(std::move(name));
        methods.insert(std::make_pair(ptr->getName(), std::move(ptr)));
    }

    /*!
         * @brief Add a read-write variable to this class
         * @details Example:
         *
         * @code
         * class Foo {
         * public:
         *     Foo(const std::string& msg) {...}
         *
         *     std::string msg;
         * };
         *
         * int main() {
         *     ...
         *     wren::VM vm;
         *     auto& m = vm.module("mymodule");
         *
         *     // Add class "Foo"
         *     auto& cls = m.klass<Foo>("Foo");
         *
         *     // Define constructor (you can only specify one constructor)
         *     cls.ctor<const std::string&>();
         *
         *     // Add class variable as a read write Wren class property
         *     cls.var<&Foo::msg>("msg");
         * }
         * @endcode
         */
    template <auto Var>
    void var(std::string name) {
        using R = typename detail::GetPointerType<decltype(Var)>::type;
        using C = typename detail::GetVarTraits<decltype(Var), Var>::klass;
        auto ptr = ForeignVarDetails<R, C, Var>::make(std::move(name), false);
        props.insert(std::make_pair(ptr->getName(), std::move(ptr)));
    }

    /*!
         * @brief Add a read-only variable to this class
         * @details This is exactly the same as var() but the variable
         * can be only read from Wren. It cannot be reassigned.
         */
    template <auto Var>
    void varReadonly(std::string name) {
        using R = typename detail::GetPointerType<decltype(Var)>::type;
        using C = typename detail::GetVarTraits<decltype(Var), Var>::klass;
        auto ptr = ForeignVarReadonlyDetails<R, C, Var>::make(std::move(name), true);
        props.insert(std::make_pair(ptr->getName(), std::move(ptr)));
    }

    /*!
         * @brief Add a read-write property to this class via a getter and a setter
         * @details This essentially creates the same thing as var()
         * but instead of using pointer to the class field, it uses getter and setter functions.
         * @code
         * class Foo {
         * public:
         *     Foo(const std::string& msg) {...}
         *     void setMsg(const std::string& msg) {...}
         *     const std::string& getMsg() const {...}
         * private:
         *     std::string msg;
         * };
         *
         * int main() {
         *     ...
         *     wren::VM vm;
         *     auto& m = vm.module("mymodule");
         *
         *     // Add class "Foo"
         *     auto& cls = m.klass<Foo>("Foo");
         *
         *     // Define constructor (you can only specify one constructor)
         *     cls.ctor<const std::string&>();
         *
         *     // Add class variable as a read write Wren class property
         *     cls.prop<&Foo::getMsg, &Foo::setMsg>("msg");
         * }
         * @endcode
         */
    template <auto Getter, auto Setter>
    void prop(std::string name) {
        auto g = ForeignGetterDetails<decltype(Getter), Getter>::method();
        auto s = ForeignSetterDetails<decltype(Setter), Setter>::method();
        auto ptr = std::make_unique<ForeignProp>(std::move(name), g, s, false);
        props.insert(std::make_pair(ptr->getName(), std::move(ptr)));
    }

    /*!
         * @brief Add a read-onlu property to this class via a getter
         * @details This essentially creates the same thing as varReadonly()
         * but instead of using pointer to the class field, it uses a getter.
         * This property will be read only and cannot be reassigned from Wren.
         * @code
         * class Foo {
         * public:
         *     Foo(const std::string& msg) {...}
         *     void setMsg(const std::string& msg) {...}
         *     const std::string& getMsg() const {...}
         * private:
         *     std::string msg;
         * };
         *
         * int main() {
         *     ...
         *     wren::VM vm;
         *     auto& m = vm.module("mymodule");
         *
         *     // Add class "Foo"
         *     auto& cls = m.klass<Foo>("Foo");
         *
         *     // Define constructor (you can only specify one constructor)
         *     cls.ctor<const std::string&>();
         *
         *     // Add class variable as a read only Wren class property
         *     cls.propReadonly<&Foo::getMsg>("msg");
         * }
         * @endcode
         */
    template <auto Getter>
    void propReadonly(std::string name) {
        auto g = ForeignGetterDetails<decltype(Getter), Getter>::method();
        auto ptr = std::make_unique<ForeignProp>(std::move(name), g, nullptr, false);
        props.insert(std::make_pair(ptr->getName(), std::move(ptr)));
    }

    /*!
         * @brief Add a read-write property to this class via a getter and a setter
         * @see prop
         * @details This essentially creates the same thing as var()
         * but instead of using pointer to the class field, it uses a static getter and setter.
         * The setter and getter do not have to belong to the class itself, but must be
         * static functions, and must accept the class as a reference as a first parameter.
         * @code
         * class Foo {
         * public:
         *     Foo(const std::string& msg) {...}
         *     std::string msg;
         * };
         *
         * static void fooSetMsg(Foo& self, const std::string& msg) {
         *     self.msg = msg;
         * }
         *
         * static const std::string& msg fooGetMsg(Foo& self) {
         *     return self.msg;
         * }
         *
         * int main() {
         *     ...
         *     wren::VM vm;
         *     auto& m = vm.module("mymodule");
         *
         *     // Add class "Foo"
         *     auto& cls = m.klass<Foo>("Foo");
         *
         *     // Define constructor (you can only specify one constructor)
         *     cls.ctor<const std::string&>();
         *
         *     // Add class variable as a read write Wren class property
         *     cls.propExt<&fooGetMsg, &fooSetMsg>("msg");
         * }
         * @endcode
         */
    template <auto Getter, auto Setter>
    void propExt(std::string name) {
        auto g = ForeignGetterExtDetails<decltype(Getter), Getter>::method();
        auto s = ForeignSetterExtDetails<decltype(Setter), Setter>::method();
        auto ptr = std::make_unique<ForeignProp>(std::move(name), g, s, false);
        props.insert(std::make_pair(ptr->getName(), std::move(ptr)));
    }

    /*!
         * @brief Add a read-only property to this class via a getter
         * @see propReadonly
         * @details This essentially creates the same thing as varReadonly()
         * but instead of using pointer to the class field, it uses a static getter.
         * The setter and does not have to belong to the class itself, but must be
         * static function, and must accept the class as a reference as a first parameter.
         * @code
         * class Foo {
         * public:
         *     Foo(const std::string& msg) {...}
         *     std::string msg;
         * };
         *
         * static const std::string& msg fooGetMsg(Foo& self) {
         *     return self.msg;
         * }
         *
         * int main() {
         *     ...
         *     wren::VM vm;
         *     auto& m = vm.module("mymodule");
         *
         *     // Add class "Foo"
         *     auto& cls = m.klass<Foo>("Foo");
         *
         *     // Define constructor (you can only specify one constructor)
         *     cls.ctor<const std::string&>();
         *
         *     // Add class variable as a read only Wren class property
         *     cls.propReadonlyExt<&fooGetMsg>("msg");
         * }
         * @endcode
         */
    template <auto Getter>
    void propReadonlyExt(std::string name) {
        auto g = ForeignGetterExtDetails<decltype(Getter), Getter>::method();
        auto ptr = std::make_unique<ForeignProp>(std::move(name), g, nullptr, false);
        props.insert(std::make_pair(ptr->getName(), std::move(ptr)));
    }
};
} // namespace wrenbind17
