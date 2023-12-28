#pragma once

#include <atomic>

#include "gutil/macros.h"

namespace starrocks {
template <class Counter>
class AtomicToken {
public:
    AtomicToken() : _counter(nullptr) {}
    AtomicToken(Counter* counter) : _counter(counter) {}
    AtomicToken(AtomicToken&& other) noexcept : _counter(other._counter) { other._counter = nullptr; }
    ~AtomicToken() {
        if (_counter) {
            (*_counter)--;
        }
        _counter = nullptr;
    }
    void operator=(AtomicToken&&) = delete;
    DISALLOW_COPY(AtomicToken);
    operator bool() const { return _counter != nullptr; }

private:
    Counter* _counter;
};
} // namespace starrocks