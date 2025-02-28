// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <initializer_list>
#include <type_traits>
#include <vector>

#include "logging.h"

namespace starrocks {

template <typename Derived>
class Cow {
    typedef std::atomic<uint32_t> AtomicCounter;

protected:
    Cow() : _use_count(0) {}
    Cow(Cow const&) : _use_count(0) {}
    Cow& operator=(Cow const&) { return *this; }

    void add_ref() { ++_use_count; }
    void release_ref() {
        if (--_use_count == 0) {
            delete static_cast<const Derived*>(this);
        }
    }

    Derived* derived() { return static_cast<Derived*>(this); }
    const Derived* derived() const { return static_cast<const Derived*>(this); }

    template <typename T>
    class Owner {
    public:
        Owner() : _t(nullptr) {}
        Owner(T* t, bool add_ref = true) : _t(t) {
            if (_t && add_ref) {
                ((std::remove_const_t<T>*)_t)->add_ref();
            }
        }
        ~Owner() {
            if (_t) {
                ((std::remove_const_t<T>*)_t)->release_ref();
            }
        }

        template <class U>
        friend class Owner;

        /// Copy construct/assignment
        template <typename U>
        Owner(Owner<U> const& rhs) : _t(rhs.get()) {
            if (_t) {
                ((std::remove_const_t<T>*)_t)->add_ref();
            }
        }
        Owner(Owner const& rhs) : _t(rhs.get()) {
            if (_t) {
                ((std::remove_const_t<T>*)_t)->add_ref();
            }
        }
        template <typename U>
        Owner& operator=(Owner<U> const& rhs) {
            Owner(rhs).swap(*this);
            return *this;
        }
        Owner& operator=(Owner const& rhs) {
            Owner(rhs).swap(*this);
            return *this;
        }
        Owner& operator=(T* rhs) {
            Owner(rhs).swap(*this);
            return *this;
        }

        /// Move onstruct/assignment
        Owner(Owner&& rhs) : _t(rhs._t) { rhs._t = nullptr; }
        template <class U>
        Owner(Owner<U>&& rhs) : _t(rhs._t) {
            rhs._t = nullptr;
        }
        Owner& operator=(Owner&& rhs) {
            Owner(static_cast<Owner&&>(rhs)).swap(*this);
            return *this;
        }
        template <class U>
        Owner& operator=(Owner<U>&& rhs) {
            Owner(static_cast<Owner<U>&&>(rhs)).swap(*this);
            return *this;
        }

        T* get() const { return _t; }
        T& operator*() const& { return *_t; }
        T&& operator*() const&& { return const_cast<std::remove_const_t<T>&&>(*_t); }
        T* operator->() const { return _t; }
        operator bool() const { return _t != nullptr; }
        operator T*() const { return _t; }

        /// @brief swap the pointer with others
        void swap(Owner& rhs) {
            T* tmp = _t;
            _t = rhs._t;
            rhs._t = tmp;
        }
        /// @brief detach and return the pointer
        T* detach() {
            T* ret = _t;
            _t = nullptr;
            return ret;
        }

        /// @brief reset the pointer with others
        void reset() { Owner().swap(*this); }
        void reset(T* rhs) { Owner(rhs).swap(*this); }
        void reset(T* rhs, bool add_ref) { Owner(rhs, add_ref).swap(*this); }

    protected:
        T* _t = nullptr;
    };

    template <typename T>
    class Mutable : public Owner<T> {
    private:
        using Base = Owner<T>;

        template <typename>
        friend class Cow;
        template <typename, typename, typename>
        friend class CowFactory;

        explicit Mutable(T* ptr, bool add_ref = true) : Base(ptr, add_ref) {}

    public:
        /// Copy: not possible.
        Mutable(const Mutable&) = delete;
        /// Move: ok.
        Mutable(Mutable&&) = default;
        Mutable& operator=(Mutable&&) = default;
        /// Initializing from temporary of compatible type.
        template <typename U>
        Mutable(Mutable<U>&& other) : Base(std::move(other)) {}
        Mutable() = default;
        Mutable(std::nullptr_t) {}
    };

    template <typename T>
    class Immutable : public Owner<const T> {
    private:
        using Base = Owner<const T>;

        template <typename>
        friend class Cow;
        template <typename, typename, typename>
        friend class CowFactory;

        explicit Immutable(const T* ptr, bool add_ref = true) : Base(ptr, add_ref) {}

    public:
        /// Copy constructor/assignment
        Immutable(const Immutable&) = default;
        Immutable& operator=(const Immutable&) = default;
        template <typename U>
        Immutable(const Immutable<U>& other) : Base(other) {}

        /// Move constructor/assignment
        Immutable(Immutable&&) = default;
        Immutable& operator=(Immutable&&) = default;
        template <typename U>
        Immutable(Immutable<U>&& other) : Base(std::move(other)) {}
        template <typename U>
        Immutable(Mutable<U>&& other) : Base(std::move(other)) {}

        /// Copy from mutable ptr: not possible.
        template <typename U>
        Immutable(const Mutable<U>&) = delete;
        Immutable() = default;
        Immutable(std::nullptr_t) {}

        const T* get() const { return this->_t; }
        T* get() { return const_cast<T*>(this->_t); }

        const T* operator->() const { return this->_t; }
        T* operator->() { return const_cast<T*>(this->_t); }

        const T& operator*() const { return *get(); }
        T& operator*() { return *get(); }
    };

public:
    using MutablePtr = Mutable<Derived>;
    using Ptr = Immutable<Derived>;

protected:
    MutablePtr shallow_mutate() const {
#ifndef NDEBUG
        if (VLOG_IS_ON(1)) {
            VLOG(1) << "[Cow] trigger Cow: " << this << ", use_count=" << this->use_count() << ", try to "
                    << (this->use_count() > 1 ? "deep" : "shadow") << " clone";
        }
#endif
        if (this->use_count() > 1) {
            return derived()->clone();
        } else {
            return as_mutable_ptr();
        }
    }

    template <typename To = Derived>
    To* as_mutable_raw_ptr() const {
        return const_cast<To*>(static_cast<const To*>(derived()));
    }

public:
    uint32_t use_count() const { return _use_count.load(); }

    template <typename... Args>
    static MutablePtr create(Args&&... args) {
        return MutablePtr(new Derived(std::forward<Args>(args)...));
    }

    template <typename T>
    static MutablePtr create(std::initializer_list<T>&& arg) {
        return create(std::forward<std::initializer_list<T>>(arg));
    }

    Ptr get_ptr() const { return Ptr(derived()); }
    MutablePtr get_ptr() { return MutablePtr(derived()); }
    MutablePtr as_mutable_ptr() const { return const_cast<Cow*>(this)->get_ptr(); }

private:
    AtomicCounter _use_count;
};

template <typename Base, typename Derived, typename AncestorBase = Base>
class CowFactory : public Base {
public:
    using BasePtr = typename AncestorBase::Ptr;
    using BaseMutablePtr = typename AncestorBase::MutablePtr;
    using Ptr = typename Base::template Immutable<Derived>;
    using MutablePtr = typename Base::template Mutable<Derived>;

    // AncestorBase is root class of inheritance hierarchy
    // if Derived class is the direct subclass of the root, then AncestorBase is just the Base class
    // if Derived class is the indirect subclass of the root, Base class is parent class, and
    // AncestorBase must be the root class. because Derived class need some type information from
    // AncestorBase to override the virtual method. e.g. clone method.
    using AncestorBaseType = std::enable_if_t<std::is_base_of_v<AncestorBase, Base>, AncestorBase>;

    template <typename... Args>
    CowFactory(Args&&... args) : Base(std::forward<Args>(args)...) {}

    template <typename... Args>
    static MutablePtr create(Args&&... args) {
        return MutablePtr(new Derived(std::forward<Args>(args)...));
    }

    template <typename T>
    static MutablePtr create(std::initializer_list<T>&& arg) {
        return MutablePtr(new Derived(std::forward<std::initializer_list<T>>(arg)));
    }

    typename AncestorBaseType::MutablePtr clone() const override {
        return typename AncestorBaseType::MutablePtr(new Derived(static_cast<const Derived&>(*this)));
    }

    static MutablePtr static_pointer_cast(BaseMutablePtr&& ptr) {
        DCHECK(ptr.get() != nullptr);
        DCHECK(static_cast<Derived*>(ptr.get()) != nullptr);
        return MutablePtr(static_cast<Derived*>(ptr.detach()), false);
    }
    static Ptr static_pointer_cast(const BasePtr& ptr) {
        DCHECK(ptr.get() != nullptr);
        DCHECK(static_cast<const Derived*>(ptr.get()) != nullptr);
        return Ptr(static_cast<const Derived*>(ptr.get()));
    }
    static Ptr static_pointer_cast(BasePtr&& ptr) {
        DCHECK(ptr.get() != nullptr);
        DCHECK(static_cast<const Derived*>(ptr.get()) != nullptr);
        return Ptr(static_cast<const Derived*>(ptr.detach()), false);
    }

    /// @brief : dynamic cast base ptr to derived ptr, like std::dynamic_pointer_cast; if failed, return nullptr.
    /// @param ptr : base ptr to cast, NOTE: it will be released if cast success.
    /// @return    : derived ptr if cast success, otherwise nullptr.
    static MutablePtr dynamic_pointer_cast(BaseMutablePtr&& ptr) {
        DCHECK(ptr.get() != nullptr);
        if (auto* _p = dynamic_cast<Derived*>(ptr.detach())) {
            return MutablePtr(_p, false);
        } else {
            return Ptr();
        }
    }
    static Ptr dynamic_pointer_cast(const BasePtr& ptr) {
        DCHECK(ptr.get() != nullptr);
        if (auto* _ptr = dynamic_cast<const Derived*>(ptr.get())) {
            return Ptr(_ptr);
        } else {
            return Ptr();
        }
    }
    static Ptr dynamic_pointer_cast(BasePtr&& ptr) {
        DCHECK(ptr.get() != nullptr);
        if (auto* _ptr = dynamic_cast<const Derived*>(ptr.detach())) {
            return Ptr(_ptr, false);
        } else {
            return Ptr();
        }
    }

protected:
    MutablePtr shallow_mutate() const { return MutablePtr(static_cast<Derived*>(Base::shallow_mutate().get())); }

private:
    Derived* derived() { return static_cast<Derived*>(this); }
    const Derived* derived() const { return static_cast<const Derived*>(this); }
};

} // namespace starrocks