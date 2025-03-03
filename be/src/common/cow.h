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

// A Clone-on-write base class inspired by Clickhouse and Rust.
//
// The type Cow is a smart pointer providing clone-on-write functionality:
//   - mutable data can not be shared with others only if it's mutated, and immutable data can be shared with others.
//   - when immutable data needs mutation, it will trigger clone-on-write which only deep clones the data if it's shared
//          with others, otherwise shadow clones the data.
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
    class RCPtr {
    public:
        RCPtr() : _t(nullptr) {}
        RCPtr(T* t, bool add_ref = true) : _t(t) {
            if (_t && add_ref) {
                ((std::remove_const_t<T>*)_t)->add_ref();
            }
        }
        ~RCPtr() {
            if (_t) {
                ((std::remove_const_t<T>*)_t)->release_ref();
            }
        }

        template <class U>
        friend class RCPtr;

        // Copy construct/assignment
        template <typename U>
        RCPtr(RCPtr<U> const& rhs) : _t(rhs.get()) {
            if (_t) {
                ((std::remove_const_t<T>*)_t)->add_ref();
            }
        }
        RCPtr(RCPtr const& rhs) : _t(rhs.get()) {
            if (_t) {
                ((std::remove_const_t<T>*)_t)->add_ref();
            }
        }
        template <typename U>
        RCPtr& operator=(RCPtr<U> const& rhs) {
            RCPtr(rhs).swap(*this);
            return *this;
        }
        RCPtr& operator=(RCPtr const& rhs) {
            RCPtr(rhs).swap(*this);
            return *this;
        }
        RCPtr& operator=(T* rhs) {
            RCPtr(rhs).swap(*this);
            return *this;
        }

        // Move onstruct/assignment
        RCPtr(RCPtr&& rhs) : _t(rhs._t) { rhs._t = nullptr; }
        template <class U>
        RCPtr(RCPtr<U>&& rhs) : _t(rhs._t) {
            rhs._t = nullptr;
        }
        RCPtr& operator=(RCPtr&& rhs) {
            RCPtr(static_cast<RCPtr&&>(rhs)).swap(*this);
            return *this;
        }
        template <class U>
        RCPtr& operator=(RCPtr<U>&& rhs) {
            RCPtr(static_cast<RCPtr<U>&&>(rhs)).swap(*this);
            return *this;
        }

        T* get() const { return _t; }
        T& operator*() const& { return *_t; }
        T&& operator*() const&& { return const_cast<std::remove_const_t<T>&&>(*_t); }
        T* operator->() const { return _t; }
        operator bool() const { return _t != nullptr; }
        operator T*() const { return _t; }

        // swap the pointer with others
        void swap(RCPtr& rhs) {
            T* tmp = _t;
            _t = rhs._t;
            rhs._t = tmp;
        }
        // detach and return the pointer
        T* detach() {
            T* ret = _t;
            _t = nullptr;
            return ret;
        }

        // reset the pointer with others
        void reset() { RCPtr().swap(*this); }
        void reset(T* rhs) { RCPtr(rhs).swap(*this); }
        void reset(T* rhs, bool add_ref) { RCPtr(rhs, add_ref).swap(*this); }

    protected:
        T* _t = nullptr;
    };

    // If the owner data is mutable, it can call non-const method of derived class, but
    // one mutable data should only have one owner and it can not be shared with others.
    template <typename T>
    class MutPtr : public RCPtr<T> {
    private:
        using Base = RCPtr<T>;

        template <typename>
        friend class Cow;
        template <typename, typename, typename>
        friend class CowFactory;

        explicit MutPtr(T* ptr, bool add_ref = true) : Base(ptr, add_ref) {}

    public:
        // Copy: not possible.
        MutPtr(const MutPtr&) = delete;
        // Move: ok.
        MutPtr(MutPtr&&) = default;
        MutPtr& operator=(MutPtr&&) = default;
        // Initializing from temporary of compatible type.
        template <typename U>
        MutPtr(MutPtr<U>&& other) : Base(std::move(other)) {}
        MutPtr() = default;
        MutPtr(std::nullptr_t) {}
    };

    // If the owner data is immutable, it can only call non const methods of derived class ideally, but
    // it can be shared with others.
    //  - mutable data can be converted to immutable data only if the mutable data's onwership is transferred;
    //  - immutable data can be converted to mutable data only if clone-on-write is triggered.
    // NOTE: To be compatible with old codes, it can call non-const methods of derived class if the Immutable data
    // is not `const` or `const&` or called from `const method`; otherwise, it can only call const methods of derived class.
    template <typename T>
    class ImmutPtr : public RCPtr<const T> {
    public:
        // Copy constructor/assignment
        ImmutPtr(const ImmutPtr&) = default;
        ImmutPtr& operator=(const ImmutPtr&) = default;
        template <typename U>
        ImmutPtr(const ImmutPtr<U>& other) : Base(other) {}

        // Move constructor/assignment
        ImmutPtr(ImmutPtr&&) = default;
        ImmutPtr& operator=(ImmutPtr&&) = default;
        template <typename U>
        ImmutPtr(ImmutPtr<U>&& other) : Base(std::move(other)) {}
        template <typename U>
        ImmutPtr(MutPtr<U>&& other) : Base(std::move(other)) {}
        template <typename... Args>
        ImmutPtr(Args&&... args) : Base(std::forward<Args>(args)...) {}
        template <typename U>
        ImmutPtr(std::initializer_list<U>&& arg) : Base(std::forward<std::initializer_list<U>>(arg)) {}

        // Copy from mutable ptr: not possible.
        template <typename U>
        ImmutPtr(const MutPtr<U>&) = delete;
        ImmutPtr() = default;
        ImmutPtr(std::nullptr_t) {}

        const T* get() const { return this->_t; }
        T* get() { return const_cast<T*>(this->_t); }

        const T* operator->() const { return this->_t; }
        T* operator->() { return const_cast<T*>(this->_t); }

        const T& operator*() const { return *get(); }
        T& operator*() { return *get(); }

    private:
        using Base = RCPtr<const T>;

        template <typename>
        friend class Cow;
        template <typename, typename, typename>
        friend class CowFactory;

        explicit ImmutPtr(const T* ptr, bool add_ref = true) : Base(ptr, add_ref) {}
    };

public:
    using MutablePtr = MutPtr<Derived>;
    using Ptr = ImmutPtr<Derived>;

protected:
    // trigger clone-on-write, deep clone if the data is shared with others, otherwise shadow clone.
    MutablePtr try_mutate() const {
#ifndef NDEBUG
        if (VLOG_IS_ON(1)) {
            VLOG(1) << "[COW] trigger COW: " << this << ", use_count=" << this->use_count() << ", try to "
                    << (this->use_count() > 1 ? "deep" : "shadow") << " clone";
        }
#endif
        if (this->use_count() > 1) {
            return derived()->clone();
        } else {
            return as_mutable_ptr();
        }
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

    // cast the data as mutable ptr if it's mutable no matter it's mutable or immutable.
    // NOTE:  ptr's use_count will be added by 1, and this is not safe because the data may be shared with others.
    MutablePtr as_mutable_ptr() const { return const_cast<Cow*>(this)->get_ptr(); }

private:
    AtomicCounter _use_count;
};

template <typename Base, typename Derived, typename AncestorBase = Base>
class CowFactory : public Base {
public:
    using BasePtr = typename AncestorBase::Ptr;
    using BaseMutablePtr = typename AncestorBase::MutablePtr;
    using Ptr = typename Base::template ImmutPtr<Derived>;
    using MutablePtr = typename Base::template MutPtr<Derived>;

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

    // cast base ptr to derived ptr statically, like std::static_pointer_cast; if failed, return nullptr.
    static Ptr static_pointer_cast(const BasePtr& ptr) {
        DCHECK(ptr.get() != nullptr);
        DCHECK(static_cast<const Derived*>(ptr.get()) != nullptr);
        return Ptr(static_cast<const Derived*>(ptr.get()));
    }

    // cast base ptr to derived ptr statically, like std::static_pointer_cast; if failed, return nullptr.
    // NOTE: ptr will be released if cast success.
    static MutablePtr static_pointer_cast(BaseMutablePtr&& ptr) {
        DCHECK(ptr.get() != nullptr);
        DCHECK(static_cast<Derived*>(ptr.get()) != nullptr);
        return MutablePtr(static_cast<Derived*>(ptr.detach()), false);
    }

    // cast base ptr to derived ptr statically, like std::static_pointer_cast; if failed, return nullptr.
    // NOTE: ptr will be released if cast success.
    static Ptr static_pointer_cast(BasePtr&& ptr) {
        DCHECK(ptr.get() != nullptr);
        DCHECK(static_cast<const Derived*>(ptr.get()) != nullptr);
        return Ptr(static_cast<const Derived*>(ptr.detach()), false);
    }

    // cast base ptr to derived ptr dynamically, like std::dynamic_pointer_cast; if failed, return nullptr.
    // NOTE: ptr will be released if cast success.
    static Ptr dynamic_pointer_cast(BasePtr&& ptr) {
        DCHECK(ptr.get() != nullptr);
        if (auto* _ptr = dynamic_cast<const Derived*>(ptr.detach())) {
            return Ptr(_ptr, false);
        } else {
            return Ptr();
        }
    }

    // cast base ptr to derived ptr dynamically, like std::dynamic_pointer_cast; if failed, return nullptr.
    // NOTE: ptr will be released if cast success.
    static MutablePtr dynamic_pointer_cast(BaseMutablePtr&& ptr) {
        DCHECK(ptr.get() != nullptr);
        if (auto* _p = dynamic_cast<Derived*>(ptr.detach())) {
            return MutablePtr(_p, false);
        } else {
            return Ptr();
        }
    }

    // cast base ptr to derived ptr dynamically, like std::dynamic_pointer_cast; if failed, return nullptr.
    static Ptr dynamic_pointer_cast(const BasePtr& ptr) {
        DCHECK(ptr.get() != nullptr);
        if (auto* _ptr = dynamic_cast<const Derived*>(ptr.get())) {
            return Ptr(_ptr);
        } else {
            return Ptr();
        }
    }

protected:
    MutablePtr try_mutate() const { return MutablePtr(static_cast<Derived*>(Base::try_mutate().get())); }

private:
    Derived* derived() { return static_cast<Derived*>(this); }
    const Derived* derived() const { return static_cast<const Derived*>(this); }
};

} // namespace starrocks