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

// This file is based on code available under the Apache license here:
//  https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/COW.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <atomic>
#include <initializer_list>
#include <type_traits>
#include <vector>

#include "logging.h" // DCHECK

namespace starrocks {

/** Copy-on-write shared ptr.
  * Allows to work with shared immutable objects and sometimes unshare and mutate you own unique copy.
  *
  * Usage:
    class Column : public COW<Column>
    {
    private:
        friend class COW<Column>;
        /// Leave all constructors in private section. They will be available through 'create' method.
        Column();
        /// Provide 'clone' method. It can be virtual if you want polymorphic behaviour.
        virtual Column * clone() const;
    public:
        /// Correctly use const qualifiers in your interface.
        virtual ~Column() {}
    };
  * It will provide 'create' and 'mutate' methods.
  * And 'Ptr' and 'MutablePtr' types.
  * Ptr is refcounted pointer to immutable object.
  * MutablePtr is refcounted noncopyable pointer to mutable object.
  * MutablePtr can be assigned to Ptr through move assignment.
  *
  * 'create' method creates MutablePtr: you cannot share mutable objects.
  * To share, move-assign to immutable pointer.
  * 'mutate' method allows to create mutable noncopyable object from immutable object:
  *   either by cloning or by using directly, if it is not shared.
  * These methods are thread-safe.
  *
  * Example:
  *
    /// Creating and assigning to immutable ptr.
    Column::Ptr x = Column::create(1);
    /// Sharing single immutable object in two ptrs.
    Column::Ptr y = x;
    /// Now x and y are shared.
    /// Change value of x.
    {
        /// Creating mutable ptr. It can clone an object under the hood if it was shared.
        Column::MutablePtr mutate_x = std::move(*x).mutate();
        /// Using non-const methods of an object.
        mutate_x->set(2);
        /// Assigning pointer 'x' to mutated object.
        x = std::move(mutate_x);
    }
    /// Now x and y are unshared and have different values.
  * Note. You may have heard that COW is bad practice.
  * Actually it is, if your values are small or if copying is done implicitly.
  * This is the case for string implementations.
  *
  * In contrast, COW is intended for the cases when you need to share states of large objects,
  * (when you usually will use std::shared_ptr) but you also want precise control over modification
  * of this shared state.
  *
  * Caveats:
  * - after a call to 'mutate' method, you can still have a reference to immutable ptr somewhere.
  * - as 'mutable_ptr' should be unique, it's refcount is redundant - probably it would be better
  *   to use std::unique_ptr for it somehow.
  */
template <typename Derived>
class COW {
protected:
    COW() : _ref_counter(0) {}

    COW(COW const&) : _ref_counter(0) {}

    COW& operator=(COW const&) { return *this; }

    void add_ref() { ++_ref_counter; }

    void release_ref() {
        if (--_ref_counter == 0) {
            delete static_cast<const Derived*>(this);
        }
    }

    Derived* derived() { return static_cast<Derived*>(this); }
    const Derived* derived() const { return static_cast<const Derived*>(this); }

    template <typename T>
    class intrusive_ptr {
    public:
        intrusive_ptr() : _t(nullptr) {}

        intrusive_ptr(T* t, bool add_ref = true) : _t(t) {
            if (_t && add_ref) {
                ((std::remove_const_t<T>*)_t)->add_ref();
            }
        }

        template <typename U>
        intrusive_ptr(intrusive_ptr<U> const& rhs) : _t(rhs.get()) {
            if (_t) {
                ((std::remove_const_t<T>*)_t)->add_ref();
            }
        }

        intrusive_ptr(intrusive_ptr const& rhs) : _t(rhs.get()) {
            if (_t) {
                ((std::remove_const_t<T>*)_t)->add_ref();
            }
        }

        ~intrusive_ptr() {
            if (_t) {
                ((std::remove_const_t<T>*)_t)->release_ref();
            }
        }

        template <typename U>
        intrusive_ptr& operator=(intrusive_ptr<U> const& rhs) {
            intrusive_ptr(rhs).swap(*this);
            return *this;
        }

        intrusive_ptr(intrusive_ptr&& rhs) : _t(rhs._t) { rhs._t = nullptr; }

        intrusive_ptr& operator=(intrusive_ptr&& rhs) {
            intrusive_ptr(static_cast<intrusive_ptr&&>(rhs)).swap(*this);
            return *this;
        }

        template <class U>
        friend class intrusive_ptr;

        template <class U>
        intrusive_ptr(intrusive_ptr<U>&& rhs) : _t(rhs._t) {
            rhs._t = nullptr;
        }

        template <class U>
        intrusive_ptr& operator=(intrusive_ptr<U>&& rhs) {
            intrusive_ptr(static_cast<intrusive_ptr<U>&&>(rhs)).swap(*this);
            return *this;
        }

        intrusive_ptr& operator=(intrusive_ptr const& rhs) {
            intrusive_ptr(rhs).swap(*this);
            return *this;
        }

        intrusive_ptr& operator=(T* rhs) {
            intrusive_ptr(rhs).swap(*this);
            return *this;
        }

        void reset() { intrusive_ptr().swap(*this); }

        void reset(T* rhs) { intrusive_ptr(rhs).swap(*this); }

        void reset(T* rhs, bool add_ref) { intrusive_ptr(rhs, add_ref).swap(*this); }

        T* get() const { return _t; }

        T* detach() {
            T* ret = _t;
            _t = nullptr;
            return ret;
        }

        void swap(intrusive_ptr& rhs) {
            T* tmp = _t;
            _t = rhs._t;
            rhs._t = tmp;
        }

        T& operator*() const& { return *_t; }

        T&& operator*() const&& { return const_cast<std::remove_const_t<T>&&>(*_t); }

        T* operator->() const { return _t; }

        operator bool() const { return _t != nullptr; }

        operator T*() const { return _t; }

    protected:
        T* _t = nullptr;
    };

    template <typename T>
    class mutable_ptr : public intrusive_ptr<T> {
    private:
        using Base = intrusive_ptr<T>;

        template <typename>
        friend class COW;
        template <typename, typename, typename>
        friend class COWHelper;

        explicit mutable_ptr(T* ptr, bool add_ref = true) : Base(ptr, add_ref) {}

    public:
        /// Copy: not possible.
        mutable_ptr(const mutable_ptr&) = delete;

        /// Move: ok.
        mutable_ptr(mutable_ptr&&) = default;
        mutable_ptr& operator=(mutable_ptr&&) = default;

        /// Initializing from temporary of compatible type.
        template <typename U>
        mutable_ptr(mutable_ptr<U>&& other) : Base(std::move(other)) {}

        mutable_ptr() = default;

        mutable_ptr(std::nullptr_t) {}
    };

    template <typename T>
    class immutable_ptr : public intrusive_ptr<const T> {
    private:
        using Base = intrusive_ptr<const T>;

        template <typename>
        friend class COW;
        template <typename, typename, typename>
        friend class COWHelper;

        explicit immutable_ptr(const T* ptr, bool add_ref = true) : Base(ptr, add_ref) {}

    public:
        /// Copy from immutable ptr: ok.
        immutable_ptr(const immutable_ptr&) = default;
        immutable_ptr& operator=(const immutable_ptr&) = default;

        template <typename U>
        immutable_ptr(const immutable_ptr<U>& other) : Base(other) {}

        /// Move: ok.
        immutable_ptr(immutable_ptr&&) = default;
        immutable_ptr& operator=(immutable_ptr&&) = default;

        /// Initializing from temporary of compatible type.
        template <typename U>
        immutable_ptr(immutable_ptr<U>&& other) : Base(std::move(other)) {}

        /// Move from mutable ptr: ok.
        template <typename U>
        immutable_ptr(mutable_ptr<U>&& other) : Base(std::move(other)) {}

        /// Copy from mutable ptr: not possible.
        template <typename U>
        immutable_ptr(const mutable_ptr<U>&) = delete;

        immutable_ptr() = default;

        immutable_ptr(std::nullptr_t) {}

        const T* get() const { return this->_t; }
        // TODO(COW): remove const_cast if we can guarantee that immutable_ptr is not modified.
        // NOTE: This only to be compatible with old codes to avoid too much changes.
        T* get() { return const_cast<T*>(this->_t); }

        const T* operator->() const { return this->_t; }
        // TODO(COW): remove const_cast if we can guarantee that immutable_ptr is not modified.
        // NOTE: This only to be compatible with old codes to avoid too much changes.
        T* operator->() { return const_cast<T*>(this->_t); }

        const T& operator*() const { return *get(); }
        T& operator*() { return *get(); }
    };

    /** Use this type in class members for compositions.
      * It works as immutable_ptr if it is const and as mutable_ptr if it is non const.
      * NOTE:
      * For classes with WrappedPtr members,
      * you must reimplement 'mutate' method, so it will call 'mutate' of all subobjects (do deep mutate).
      * It will guarantee, that mutable object have all subobjects unshared.
      *
      * NOTE:
      * If you override 'mutate' method in inherited classes, don't forget to make it virtual in base class or to make it call a virtual method.
      * (COW itself doesn't force any methods to be virtual).
      *
      * See example in "cow_compositions.cpp".
      */
    template <typename T>
    class chameleon_ptr {
    private:
        immutable_ptr<T> value;

    public:
        template <typename... Args>
        chameleon_ptr(Args&&... args) : value(std::forward<Args>(args)...) {}

        template <typename U>
        chameleon_ptr(std::initializer_list<U>&& arg) : value(std::forward<std::initializer_list<U>>(arg)) {}

        const T* get() const { return value.get(); }
        T* get() { return value->template assume_mutable_ptr<T>(); }

        const T* operator->() const { return get(); }
        T* operator->() { return get(); }

        const T& operator*() const { return *value; }
        T& operator*() { return *(value->template assume_mutable_ptr<T>()); }

        operator const immutable_ptr<T>&() const { return value; }
        operator immutable_ptr<T>&() { return value; }

        /// Get internal immutable ptr. Does not change internal use counter.
        immutable_ptr<T> detach() && { return std::move(value); }

        operator bool() const { return value != nullptr; }
        bool operator!() const { return value == nullptr; }

        bool operator==(const chameleon_ptr& rhs) const { return value == rhs.value; }
        bool operator!=(const chameleon_ptr& rhs) const { return value != rhs.value; }
    };

public:
    using MutablePtr = mutable_ptr<Derived>;
    using Ptr = immutable_ptr<Derived>;
    using WrappedPtr = chameleon_ptr<Derived>;

protected:
    MutablePtr shallow_mutate() const {
        if (this->use_count() > 1) {
            VLOG(1) << "[COW] trigger COW: " << this << ", use_count=" << this->use_count() << ", try to deep clone";
            return derived()->clone();
        } else {
            VLOG(1) << "[COW] trigger COW: " << this << ", use_count=" << this->use_count() << ", try to shadow clone";
            return assume_mutable();
        }
    }

public:
    uint32_t use_count() const { return _ref_counter.load(); }

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

    MutablePtr mutate() const&& { return shallow_mutate(); }

    MutablePtr assume_mutable() const { return const_cast<COW*>(this)->get_ptr(); }

    template <typename To = Derived>
    To& assume_mutable_ref() const {
        return const_cast<To&>(static_cast<const To&>(derived()));
    }

    template <typename To = Derived>
    To* assume_mutable_ptr() const {
        return const_cast<To*>(static_cast<const To*>(derived()));
    }

private:
    std::atomic<uint32_t> _ref_counter;
};

/** Helper class to support inheritance.
  * Example:
  *
  * class IColumn : public COW<IColumn>
  * {
  *     friend class COW<IColumn>;
  *     virtual MutablePtr clone() const = 0;
  *     virtual ~IColumn() {}
  * };
  *
  * class ConcreteColumn : public COWHelper<IColumn, ConcreteColumn>
  * {
  *     friend class COWHelper<IColumn, ConcreteColumn>;
  * };
  *
  * Here is complete inheritance diagram:
  *
  * ConcreteColumn
  *  COWHelper<IColumn, ConcreteColumn>
  *   IColumn
  *    CowPtr<IColumn>
  *     boost::intrusive_ref_counter<IColumn>
  *
  * See example in "cow_columns.cpp".
  */
template <typename Base, typename Derived, typename AncestorBase = Base>
class COWHelper : public Base {
public:
    using BasePtr = typename AncestorBase::Ptr;
    using BaseMutablePtr = typename AncestorBase::MutablePtr;
    using Ptr = typename Base::template immutable_ptr<Derived>;
    using MutablePtr = typename Base::template mutable_ptr<Derived>;
    using DerivedWrappedPtr = typename Base::template chameleon_ptr<Derived>;

    // AncestorBase is root class of inheritance hierarchy
    // if Derived class is the direct subclass of the root, then AncestorBase is just the Base class
    // if Derived class is the indirect subclass of the root, Base class is parent class, and
    // AncestorBase must be the root class. because Derived class need some type information from
    // AncestorBase to override the virtual method. e.g. clone method.
    using AncestorBaseType = std::enable_if_t<std::is_base_of_v<AncestorBase, Base>, AncestorBase>;

    template <typename... Args>
    COWHelper(Args&&... args) : Base(std::forward<Args>(args)...) {}

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