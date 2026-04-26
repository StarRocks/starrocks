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

#include <algorithm>

namespace starrocks {
template <typename T, typename Sequence, typename Compare>
class SortingHeap {
public:
    SortingHeap(Compare comp) : _comp(std::move(comp)) {}

    const T& top() { return _queue.front(); }

    size_t size() { return _queue.size(); }

    bool empty() { return _queue.empty(); }

    T& next_child() { return _queue[_greater_child_index()]; }

    void reserve(size_t reserve_sz) { _queue.reserve(reserve_sz); }

    void replace_top(T&& new_top) {
        *_queue.begin() = std::move(new_top);
        update_top();
    }

    void remove_top() {
        std::pop_heap(_queue.begin(), _queue.end(), _comp);
        _queue.pop_back();
    }

    void push(T&& rowcur) {
        _queue.emplace_back(std::move(rowcur));
        std::push_heap(_queue.begin(), _queue.end(), _comp);
    }

    Sequence&& sorted_seq() {
        std::sort_heap(_queue.begin(), _queue.end(), _comp);
        return std::move(_queue);
    }

    Sequence& container() { return _queue; }

    // replace top if val less than top()
    void replace_top_if_less(T&& val) {
        if (_comp(val, top())) {
            replace_top(std::move(val));
        }
    }

private:
    Sequence _queue;
    Compare _comp;

    size_t _greater_child_index() {
        size_t next_idx = 0;
        if (next_idx == 0) {
            next_idx = 1;
            if (_queue.size() > 2 && _comp(_queue[1], _queue[2])) ++next_idx;
        }
        return next_idx;
    }

    void update_top() {
        size_t size = _queue.size();
        if (size < 2) return;

        auto begin = _queue.begin();

        size_t child_idx = _greater_child_index();
        auto child_it = begin + child_idx;

        /// Check if we are in order.
        if (_comp(*child_it, *begin)) return;

        auto curr_it = begin;
        auto top(std::move(*begin));
        do {
            /// We are not in heap-order, swap the parent with it's largest child.
            *curr_it = std::move(*child_it);
            curr_it = child_it;

            // recompute the child based off of the updated parent
            child_idx = 2 * child_idx + 1;

            if (child_idx >= size) break;

            child_it = begin + child_idx;

            if ((child_idx + 1) < size && _comp(*child_it, *(child_it + 1))) {
                /// Right child exists and is greater than left child.
                ++child_it;
                ++child_idx;
            }

            /// Check if we are in order.
        } while (!(_comp(*child_it, top)));
        *curr_it = std::move(top);
    }
};
} // namespace starrocks