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

#include <fmt/format.h>

#include <algorithm>
#include <boost/type_index.hpp>
#include <cstddef>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace starrocks {

class SerializeHelpers {
public:
    template <typename T>
    static void serialize(const T* data, uint8_t*& buffer, size_t num_elems = 1) {
        using T_ = std::remove_cv_t<std::remove_reference_t<T>>;
        std::copy_n(reinterpret_cast<const uint8_t*>(data), num_elems * sizeof(T), buffer);
        buffer += num_elems * sizeof(T);
    }

    template <typename T>
    static void deserialize(const uint8_t*& buffer, T* data, size_t num_elems = 1) {
        using T_ = std::remove_cv_t<std::remove_reference_t<T>>;
        std::copy_n(buffer, num_elems * sizeof(T), reinterpret_cast<uint8_t*>(data));
        buffer += num_elems * sizeof(T);
    }

    template <typename T>
    static void serialize(const T& data, uint8_t*& buffer) {
        serialize(&data, buffer, 1);
    }

    template <typename T>
    static void deserialize(const uint8_t*& buffer, T& data) {
        deserialize(buffer, &data, 1);
    }

    static void serialize(const std::string& data, uint8_t*& buffer) {
        size_t size = data.length();
        serialize(size, buffer);
        serialize(data.data(), buffer, size);
    }

    static void deserialize(const uint8_t*& buffer, std::string& data) {
        size_t size = 0;
        deserialize(buffer, size);
        data.resize(size);
        deserialize(buffer, data.data(), size);
    }

    template <typename T>
    static void serialize(std::vector<T> const& data, uint8_t*& buffer) {
        size_t size = data.size();
        serialize(size, buffer);
        for (auto const& elem : data) {
            serialize(elem, buffer);
        }
    }

    template <typename T>
    static void deserialize(const uint8_t*& buffer, std::vector<T>& data) {
        size_t size = 0;
        deserialize(buffer, size);
        data.resize(size);
        for (auto& elem : data) {
            deserialize(buffer, elem);
        }
    }

    template <typename MapType>
    static typename std::enable_if_t<
            std::is_same_v<MapType, std::map<typename MapType::key_type, typename MapType::mapped_type>> ||
                    std::is_same_v<MapType,
                                   std::unordered_map<typename MapType::key_type, typename MapType::mapped_type>>,
            void>
    serialize(MapType const& data, uint8_t*& buffer) {
        size_t size = data.size();
        serialize(size, buffer);
        for (auto const& [key, value] : data) {
            serialize(key, buffer);
            serialize(value, buffer);
        }
    }

    template <typename MapType>
    static typename std::enable_if_t<
            std::is_same_v<MapType, std::map<typename MapType::key_type, typename MapType::mapped_type>> ||
                    std::is_same_v<MapType,
                                   std::unordered_map<typename MapType::key_type, typename MapType::mapped_type>>,
            void>
    deserialize(const uint8_t*& buffer, MapType& data) {
        size_t size = 0;
        deserialize(buffer, size);
        for (size_t i = 0; i < size; ++i) {
            typename MapType::key_type key;
            typename MapType::mapped_type value;
            deserialize(buffer, key);
            deserialize(buffer, value);
            data[std::move(key)] = std::move(value);
        }
    }

    template <typename SetType>
    static typename std::enable_if_t<std::is_same_v<SetType, std::set<typename SetType::key_type>> ||
                                             std::is_same_v<SetType, std::unordered_set<typename SetType::key_type>>,
                                     void>
    serialize(SetType const& data, uint8_t*& buffer) {
        size_t size = data.size();
        serialize(size, buffer);
        for (auto const& key : data) {
            serialize(key, buffer);
        }
    }

    template <typename SetType>
    static typename std::enable_if_t<std::is_same_v<SetType, std::set<typename SetType::key_type>> ||
                                             std::is_same_v<SetType, std::unordered_set<typename SetType::key_type>>,
                                     void>
    deserialize(const uint8_t*& buffer, SetType& data) {
        size_t size = 0;
        deserialize(buffer, size);
        for (size_t i = 0; i < size; ++i) {
            typename SetType::key_type key;
            deserialize(buffer, key);
            data.emplace(std::move(key));
        }
    }

    template <typename ElemType, size_t Length>
    static void serialize(std::array<ElemType, Length> const& data, uint8_t*& buffer) {
        for (auto const& elem : data) {
            serialize(elem, buffer);
        }
    }

    template <typename ElemType, size_t Length>
    static void deserialize(const uint8_t*& buffer, std::array<ElemType, Length>& data) {
        for (auto& elem : data) {
            deserialize(buffer, elem);
        }
    }

    template <typename T>
    static size_t serialized_size(T const&) {
        return sizeof(T);
    }

    static size_t serialized_size(std::string const& str) { return sizeof(size_t) + sizeof(char) * str.length(); }

    template <typename T>
    static size_t serialized_size(std::vector<T> const& vec) {
        size_t size = sizeof(size_t);
        for (auto const& elem : vec) {
            size += serialized_size(elem);
        }
        return size;
    }

    template <typename T, size_t Length>
    static size_t serialized_size(std::array<T, Length> const& vec) {
        size_t size = 0;
        for (auto const& elem : vec) {
            size += serialized_size(elem);
        }
        return size;
    }

    template <typename MapType>
    static typename std::enable_if_t<
            std::is_same_v<MapType, std::map<typename MapType::key_type, typename MapType::mapped_type>> ||
                    std::is_same_v<MapType,
                                   std::unordered_map<typename MapType::key_type, typename MapType::mapped_type>>,
            size_t>
    serialized_size(MapType const& data) {
        size_t size = sizeof(size_t);
        for (auto const& [key, value] : data) {
            size += serialized_size(key) + serialized_size(value);
        }
        return size;
    }

    template <typename SetType>
    static typename std::enable_if_t<std::is_same_v<SetType, std::set<typename SetType::key_type>> ||
                                             std::is_same_v<SetType, std::unordered_set<typename SetType::key_type>>,
                                     size_t>
    serialized_size(SetType const& data) {
        size_t size = sizeof(size_t);
        for (auto const& elem : data) {
            size += serialized_size(elem);
        }
        return size;
    }

    template <typename... Args>
    static size_t serialized_size(std::tuple<Args...> const& tuple) {
        size_t size = 0;
        std::apply([&size](const auto&... element) { ((size += serialized_size(element)), ...); }, tuple);
        return size;
    }

    template <typename... Args>
    static void serialize(std::tuple<Args...> const& tuple, uint8_t*& buffer) {
        std::apply([&buffer](const auto&... element) { (serialize(element, buffer), ...); }, tuple);
    }

    template <typename... Args>
    static void deserialize(const uint8_t*& buffer, std::tuple<Args...>& tuple) {
        std::apply([&buffer](auto&... element) { (deserialize(buffer, element), ...); }, tuple);
    }

    template <typename First, typename... Args>
    static size_t serialized_size_all(First&& first, Args&&... rest) {
        if constexpr (sizeof...(rest) == 0) {
            return serialized_size(first);
        } else {
            return serialized_size(first) + serialized_size_all(std::forward<Args>(rest)...);
        }
    }

    template <typename First, typename... Args>
    static void serialize_all(uint8_t*& data, First const& first, Args&&... rest) {
        serialize(first, data);
        if constexpr (sizeof...(rest) != 0) {
            serialize_all(data, std::forward<Args>(rest)...);
        }
    }

    template <typename First, typename... Args>
    static void deserialize_all(const uint8_t*& data, First& first, Args&&... rest) {
        deserialize(data, first);
        if constexpr (sizeof...(rest) != 0) {
            deserialize_all(data, std::forward<Args>(rest)...);
        }
    }
};

} // namespace starrocks
