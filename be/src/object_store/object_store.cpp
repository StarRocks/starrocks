// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "object_store/object_store.h"

#include <fmt/format.h>

#include <regex>

namespace starrocks {

using FactoryFunc = ObjectStore::FactoryFunc;

class ObjectStoreRegistry {
public:
    ~ObjectStoreRegistry() = default;

    static ObjectStoreRegistry& instance() {
        static ObjectStoreRegistry obj;
        return obj;
    }

    void register_store(std::string_view pattern, FactoryFunc func) {
        _entries.emplace_back(std::regex(pattern.data(), pattern.size()), std::move(func));
    }

    StatusOr<std::unique_ptr<ObjectStore>> new_store(std::string_view uri) {
        for (const auto& [pattern, func] : _entries) {
            if (std::regex_match(uri.begin(), uri.end(), pattern)) {
                return func(uri);
            }
        }
        return Status::NotSupported(fmt::format("No object store associated with uri {}", uri));
    }

private:
    ObjectStoreRegistry() = default;

    using Entry = std::pair<std::regex, FactoryFunc>;
    std::vector<Entry> _entries;
};

void ObjectStore::register_store(std::string_view pattern, FactoryFunc func) {
    ObjectStoreRegistry::instance().register_store(pattern, std::move(func));
}

StatusOr<std::unique_ptr<ObjectStore>> ObjectStore::create_unique_from_uri(std::string_view uri) {
    return ObjectStoreRegistry::instance().new_store(uri);
}

} // namespace starrocks
