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

#include <Poco/Timespan.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>

namespace starrocks::poco {

template <typename Object>
class PoolBase {
public:
    using ObjectPtr = std::shared_ptr<Object>;

private:
    struct PooledObject {
        PooledObject(ObjectPtr object, PoolBase& pool) : object(object), pool(pool) {}

        ObjectPtr object;
        bool in_use = false;
        bool in_good_condition = true;
        PoolBase& pool;
    };

    struct PoolEntryHelper {
        explicit PoolEntryHelper(PooledObject& data) : data(data) { data.in_use = true; }
        ~PoolEntryHelper() {
            std::unique_lock lock(data.pool._mutex);
            data.in_use = false;
            data.pool._available.notify_one();
        }

        PooledObject& data;
    };

public:
    class Entry {
    public:
        friend class PoolBase<Object>;

        Entry() = default;

        Object* operator->() && = delete;
        const Object* operator->() const&& = delete;
        Object& operator*() && = delete;
        const Object& operator*() const&& = delete;

        Object* operator->() & { return &*_data->data.object; }
        const Object* operator->() const& { return &*_data->data.object; }
        Object& operator*() & { return *_data->data.object; }
        const Object& operator*() const& { return *_data->data.object; }

        PoolBase* getPool() const { return &_data->data.pool; }

        void set_not_in_good_condition() { _data->data.in_good_condition = false; }

    private:
        std::shared_ptr<PoolEntryHelper> _data;

        explicit Entry(PooledObject& object) : _data(std::make_shared<PoolEntryHelper>(object)) {}
    };

    PoolBase(size_t max_items) : _max_items(max_items) { _items.reserve(_max_items); }

    virtual ~PoolBase() = default;

    Entry get(Poco::Timespan::TimeDiff timeout) {
        std::unique_lock lock(_mutex);

        while (true) {
            for (auto& item : _items) {
                if (!item->in_use) {
                    if (!item->in_good_condition) {
                        ObjectPtr object = allocObject();
                        item = std::make_shared<PooledObject>(object, *this);
                    }
                    return Entry(*item);
                }
            }

            if (_items.size() < _max_items) {
                ObjectPtr object = allocObject();
                _items.emplace_back(std::make_shared<PooledObject>(object, *this));
                return Entry(*_items.back());
            }

            if (timeout < 0) {
                _available.wait(lock);
            } else {
                _available.wait_for(lock, std::chrono::microseconds(timeout));
            }
        }
    }

    void reserve(size_t count) {
        std::unique_lock lock(_mutex);
        while (_items.size() < count) {
            _items.emplace_back(std::make_shared<PooledObject>(allocObject(), *this));
        }
    }

protected:
    virtual ObjectPtr allocObject() = 0;

private:
    const size_t _max_items;
    std::vector<std::shared_ptr<PooledObject>> _items;

    mutable std::mutex _mutex;
    std::condition_variable _available;
};

} // namespace starrocks::poco
