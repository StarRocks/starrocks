#pragma once

//#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/detail/atomic_count.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <initializer_list>

namespace starrocks {

namespace vectorized {

template <typename Derived>
class COWCounter;

template <typename Derived>
void intrusive_ptr_add_ref(const COWCounter<Derived> *p);

template <typename Derived>
void intrusive_ptr_release(const COWCounter<Derived> *p);

template <typename Derived>
class COWCounter
{
    typedef boost::detail::atomic_count type;
private:
    mutable type m_ref_counter;
    mutable bool _pool;

protected:
    mutable size_t _chunk_size;

public:
    COWCounter(): m_ref_counter(0), _pool(false) {}
    COWCounter(bool pool, size_t chunk_size): m_ref_counter(0), _pool(pool), _chunk_size(chunk_size) {}
    
    COWCounter(COWCounter const&): m_ref_counter(0) {}
    COWCounter& operator=(COWCounter const&) { return *this; }
    unsigned int use_count() const
    {
        return static_cast< unsigned int >(static_cast< long >(m_ref_counter));
    }

public:
    virtual void return_to_pool() const = 0;
    void set_chunk_size(size_t chunk_size) { _chunk_size = chunk_size; }
    void set_pool(bool pool) { _pool = pool; } 

protected:
    friend void intrusive_ptr_add_ref<Derived>(const COWCounter<Derived> *p);
    friend void intrusive_ptr_release<Derived>(const COWCounter<Derived> *p);
};

template <typename Derived>
inline void intrusive_ptr_add_ref(const COWCounter<Derived> *p) {
    ++p->m_ref_counter;
}

template <typename Derived>
inline void intrusive_ptr_release(const COWCounter<Derived> *p) {
    if (static_cast< unsigned int >(--p->m_ref_counter) == 0) {
        if (!p->_pool) {
            delete static_cast< const Derived* >(p);
        } else {
            p->return_to_pool();
        }
    }
}


template <typename Derived>
class COW: public COWCounter<Derived> { //public boost::intrusive_ref_counter<Derived> {
private:
    Derived *derived() { return static_cast<Derived *>(this); }
    const Derived *derived() const { return static_cast<const Derived *>(this); }

public:
    COW(): COWCounter<Derived>() {}
    COW(bool pool, size_t chunk_size): COWCounter<Derived>(pool, chunk_size) {}

protected:
    template <typename T>
    class mutable_ptr: public boost::intrusive_ptr<T> {
    private:
        using Base = boost::intrusive_ptr<T>;

        template <typename> friend class COW;
        template <typename, typename, typename> friend class ColumnFactory;

        explicit mutable_ptr(T *ptr): Base(ptr) {}
    public:
        mutable_ptr(const mutable_ptr &) = delete;
        mutable_ptr & operator=(const mutable_ptr &) = delete;

        mutable_ptr(mutable_ptr &&) = default;
        mutable_ptr & operator=(mutable_ptr &&) = default;

        template <typename U>
        mutable_ptr(mutable_ptr<U> &&other) : Base(std::move(other)) {}

        mutable_ptr() = default;
        mutable_ptr(std::nullptr_t) {}
    };
public:
    using MutablePtr = mutable_ptr<Derived>;

protected:
    template <typename T>
    class immutable_ptr: public boost::intrusive_ptr<T> {
    private:
        using Base = boost::intrusive_ptr<T>;

        template <typename> friend class COW;
        template <typename, typename, typename> friend class ColumnFactory;
        friend class ColumnHelper;

        explicit immutable_ptr(T *ptr): Base(ptr) {}
    public:
        immutable_ptr(const immutable_ptr &) = default;
        immutable_ptr & operator=(const immutable_ptr &) = default;

        template <typename U>
        immutable_ptr(const immutable_ptr<U> &other): Base(other) {}

        immutable_ptr(immutable_ptr &&) = default;
        immutable_ptr & operator=(immutable_ptr &&) = default;

        template <typename U>
        immutable_ptr(immutable_ptr<U> &&other): Base(std::move(other)) {}

        template <typename U>
        immutable_ptr(mutable_ptr<U> &&other): Base(std::move(other)) {}

        template <typename U>
        immutable_ptr(mutable_ptr<U> &other) = delete;

        immutable_ptr() = default;
        immutable_ptr(std::nullptr_t) {}
    };
public:
    using Ptr = immutable_ptr<Derived>;

    Ptr getPtr() const { return static_cast<Ptr>(derived()); }
    MutablePtr getPtr() { return static_cast<MutablePtr>(derived()); }

protected:
    MutablePtr shallowMutate() const {
        if (this->use_count() > 1) {
            return derived() -> clone();
        } else {
            return assumeMutable();
        }
    }

public:
    static MutablePtr mutate(Ptr ptr) {
        return ptr->shallowMutate();
    }

    MutablePtr assumeMutable() const {
        return const_cast<COW *>(this)->getPtr();
    }

    Derived & assumeMutableRef() const {
        return const_cast<Derived &>(*derived());
    }

    bool unique() const {
        return this->use_count() == 1;
    }

protected:
    template <typename T>
    class chameleon_ptr
    {
    private:
        immutable_ptr<T> value;

    public:
        template<typename... Args>
        chameleon_ptr(Args &&... args): value(std::forward<Args>(args)...) {}

        template<typename U>
        chameleon_ptr(std::initializer_list<U> && arg): value(std::forward<std::initializer_list<U>>(arg)) {}

        const T * get() const { return value.get(); }
        T * get() { return &value->assumeMutableRef(); }

        const T * operator->() const { return get(); }
        T * operator->() { return get(); }

        const T & operator*() const { return *value; }
        T & operator*() { return value->assumeMutableRef(); }

        operator const immutable_ptr<T> & () const { return value; }
        operator immutable_ptr<T> & () { return value; }

        immutable_ptr<T> detach() && { return std::move(value); }

        operator bool() const { return value != nullptr; }
        bool operator! () const { return value == nullptr; }

        bool operator== (const chameleon_ptr & rhs) const { return value == rhs.value; }
        bool operator!= (const chameleon_ptr & rhs) const { return value != rhs.value; }
    };

public:
    using WrappedPtr = chameleon_ptr<Derived>;
};

}
}

