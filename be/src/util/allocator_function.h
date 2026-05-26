// Allocator-aware, move-only type-erased callable.
// Small callables (<= 3 pointers, nothrow-move) live inline (no allocation).
// Larger callables are allocated through the supplied Allocator.
#pragma once

#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace starrocks {

template <class Sig, class Alloc = std::allocator<std::byte>>
class unique_function;

namespace detail::uf {

inline constexpr std::size_t inline_size  = sizeof(void*) * 3;
inline constexpr std::size_t inline_align = alignof(void*);

template <class T>
inline constexpr bool fits_inline_v =
        sizeof(T) <= inline_size && alignof(T) <= inline_align &&
        std::is_nothrow_move_constructible_v<T>;

template <class R, class... Args>
struct vtable {
    R    (*invoke)(void*, Args&&...);
    void (*relocate)(void* src, void* dst) noexcept;
    void (*destroy)(void* self, void* alloc) noexcept;
};

template <class F, class Alloc, class R, class... Args>
struct ops {
    static constexpr bool inlined = fits_inline_v<F>;

    using AT  = std::allocator_traits<Alloc>;
    using FA  = typename AT::template rebind_alloc<F>;
    using FAT = std::allocator_traits<FA>;

    static R invoke(void* self, Args&&... args) {
        F* fp = inlined ? static_cast<F*>(self) : *static_cast<F**>(self);
        return (*fp)(std::forward<Args>(args)...);
    }

    static void relocate(void* src, void* dst) noexcept {
        if constexpr (inlined) {
            ::new (dst) F(std::move(*static_cast<F*>(src)));
            static_cast<F*>(src)->~F();
        } else {
            *static_cast<F**>(dst) = *static_cast<F**>(src);
            *static_cast<F**>(src) = nullptr;
        }
    }

    static void destroy(void* self, void* alloc_void) noexcept {
        if constexpr (inlined) {
            static_cast<F*>(self)->~F();
        } else {
            F* p = *static_cast<F**>(self);
            if (!p) return;
            FA a(*static_cast<Alloc*>(alloc_void));
            FAT::destroy(a, p);
            FAT::deallocate(a, p, 1);
        }
    }

    static constexpr vtable<R, Args...> table{&invoke, &relocate, &destroy};
};

} // namespace detail::uf

template <class R, class... Args, class Alloc>
class unique_function<R(Args...), Alloc> {
    using vt_t = detail::uf::vtable<R, Args...>;

public:
    using result_type    = R;
    using allocator_type = Alloc;

    unique_function() noexcept = default;
    explicit unique_function(const Alloc& a) noexcept : _alloc(a) {}

    template <class F, class DF = std::decay_t<F>,
              std::enable_if_t<!std::is_same_v<DF, unique_function> &&
                                       std::is_invocable_r_v<R, DF&, Args...>,
                               int> = 0>
    unique_function(F&& f, const Alloc& a = Alloc{}) : _alloc(a) {
        _emplace<DF>(std::forward<F>(f));
    }

    unique_function(unique_function&& other) noexcept
            : _alloc(std::move(other._alloc)), _vt(other._vt) {
        if (_vt) {
            _vt->relocate(&other._storage, &_storage);
            other._vt = nullptr;
        }
    }

    unique_function& operator=(unique_function&& other) noexcept {
        if (this != &other) {
            reset();
            _alloc = std::move(other._alloc);
            _vt    = other._vt;
            if (_vt) {
                _vt->relocate(&other._storage, &_storage);
                other._vt = nullptr;
            }
        }
        return *this;
    }

    unique_function(const unique_function&)            = delete;
    unique_function& operator=(const unique_function&) = delete;

    ~unique_function() { reset(); }

    void reset() noexcept {
        if (_vt) {
            _vt->destroy(&_storage, &_alloc);
            _vt = nullptr;
        }
    }

    explicit operator bool() const noexcept { return _vt != nullptr; }

    R operator()(Args... args) const {
        return _vt->invoke(const_cast<void*>(static_cast<const void*>(&_storage)),
                           std::forward<Args>(args)...);
    }

    allocator_type get_allocator() const noexcept { return _alloc; }

private:
    template <class DF, class F>
    void _emplace(F&& f) {
        using Ops = detail::uf::ops<DF, Alloc, R, Args...>;
        if constexpr (Ops::inlined) {
            ::new (&_storage) DF(std::forward<F>(f));
        } else {
            using FA  = typename std::allocator_traits<Alloc>::template rebind_alloc<DF>;
            using FAT = std::allocator_traits<FA>;
            FA a(_alloc);
            DF* p = FAT::allocate(a, 1);
            try {
                FAT::construct(a, p, std::forward<F>(f));
            } catch (...) {
                FAT::deallocate(a, p, 1);
                throw;
            }
            *reinterpret_cast<DF**>(&_storage) = p;
        }
        _vt = &Ops::table;
    }

    alignas(detail::uf::inline_align) std::byte _storage[detail::uf::inline_size]{};
    const vt_t* _vt = nullptr;
    [[no_unique_address]] Alloc _alloc{};
};

// Convenience alias for std::pmr.
template <class Sig>
using pmr_unique_function =
        unique_function<Sig, std::pmr::polymorphic_allocator<std::byte>>;

} // namespace starrocks
