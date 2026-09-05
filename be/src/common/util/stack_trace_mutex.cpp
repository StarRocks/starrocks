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

#include <array>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

namespace starrocks {

namespace {

class CircularBuffer {
public:
    void push(std::string s);

    void list(std::vector<std::string>* res);

private:
    constexpr static const int kBufferSize = 12;

    void pop();

    mutable std::mutex _mtx;
    std::array<std::string, kBufferSize> _buffer;
    int _elements = 0;
    int _read_pos = 0;
    int _write_pos = 0;
};

inline void CircularBuffer::push(std::string s) {
    std::lock_guard l(_mtx);
    if (_elements == kBufferSize) {
        pop();
    }
    _buffer[_write_pos % kBufferSize] = std::move(s);
    _elements++;
    _write_pos++;
    if (_write_pos == kBufferSize) {
        _write_pos = 0;
    }
}

inline void CircularBuffer::pop() {
    _elements--;
    _read_pos++;
    if (_read_pos == kBufferSize) {
        _read_pos = 0;
    }
}

inline void CircularBuffer::list(std::vector<std::string>* res) {
    std::lock_guard l(_mtx);
    res->reserve(_elements);
    for (int i = 0; i < _elements; i++) {
        res->emplace_back(_buffer[(_read_pos + i) % kBufferSize]);
    }
}

CircularBuffer g_buffer;

} // namespace

void save_stack_trace_of_long_wait_mutex(const std::string& stack_trace) {
    g_buffer.push(stack_trace);
}

std::vector<std::string> list_stack_trace_of_long_wait_mutex() {
    std::vector<std::string> res;
    g_buffer.list(&res);
    return res;
}

} // namespace starrocks
