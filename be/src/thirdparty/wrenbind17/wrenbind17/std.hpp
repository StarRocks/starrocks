#pragma once

#include <algorithm>
#include <list>
#include <map>
#include <unordered_map>
#include <vector>

#include "module.hpp"

namespace wrenbind17 {
#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace detail {
template <typename T, typename = void>
struct is_equality_comparable : std::false_type {};

template <typename T>
struct is_equality_comparable<
        T, typename std::enable_if<true, decltype(std::declval<T&>() == std::declval<T&>(), (void)0)>::type>
        : std::true_type {};
} // namespace detail

template <typename T, typename T2 = void>
class StdVectorHelper;

template <typename T>
class StdVectorHelper<T, typename std::enable_if<detail::is_equality_comparable<T>::value>::type> {
public:
    static bool contains(std::vector<T>& self, const T& value) {
        return std::find(self.begin(), self.end(), value) != self.end();
    }
};

template <typename T>
class StdVectorHelper<T, typename std::enable_if<!detail::is_equality_comparable<T>::value>::type> {
public:
    static bool contains(std::vector<T>& self, const T& value) {
        return std::find_if(self.begin(), self.end(), [&](const T& e) -> bool { return &e == &value; }) != self.end();
    }
};
#endif

template <typename T>
class StdVectorBindings {
public:
    typedef typename std::vector<T>::iterator Iterator;
    typedef typename std::vector<T> Vector;

    static void setIndex(Vector& self, size_t index, T value) { self[index] = std::move(value); }

    static const T& getIndex(Vector& self, size_t index) { return self[index]; }

    static void add(Vector& self, T value) { self.push_back(std::move(value)); }

    static std::variant<bool, Iterator> iterate(Vector& self, std::variant<std::nullptr_t, Iterator> other) {
        if (other.index() == 1) {
            auto it = std::get<Iterator>(other);
            ++it;
            if (it != self.end()) {
                return {it};
            }

            return {false};
        } else {
            if (self.empty()) return {false};
            return {self.begin()};
        }
    }

    static const T& iteratorValue(Vector& self, std::shared_ptr<Iterator> other) {
        auto& it = *other;
        return *it;
    }

    static size_t count(Vector& self) { return self.size(); }

    static T removeAt(Vector& self, int32_t index) {
        if (index == -1) {
            auto ret = std::move(self.back());
            self.pop_back();
            return std::move(ret);
        } else {
            if (index < 0) {
                index = static_cast<int32_t>(self.size()) + index;
            }

            if (index > static_cast<int32_t>(self.size())) {
                throw std::out_of_range("invalid index");
            } else if (index == static_cast<int32_t>(self.size())) {
                auto ret = std::move(self.back());
                self.pop_back();
                return std::move(ret);
            } else {
                auto ret = std::move(self.at(index));
                self.erase(self.begin() + index);
                return std::move(ret);
            }
        }
    }

    static void insert(Vector& self, int32_t index, T value) {
        if (index == -1) {
            self.push_back(std::move(value));
        } else {
            if (index < 0) {
                index = static_cast<int32_t>(self.size()) + index;
            }

            if (index > static_cast<int32_t>(self.size())) {
                throw std::out_of_range("invalid index");
            } else if (index == static_cast<int32_t>(self.size())) {
                self.push_back(std::move(value));
            } else {
                auto it = self.begin() + index;
                self.insert(it, std::move(value));
            }
        }
    }

    static bool contains(Vector& self, const T& value) {
        // return std::find(self.begin(), self.end(), value) != self.end();
        return StdVectorHelper<T>::contains(self, value);
    }

    static T pop(Vector& self) {
        auto ret = std::move(self.back());
        self.pop_back();
        return std::move(ret);
    }

    static void clear(Vector& self) { self.clear(); }

    static size_t size(Vector& self) { return self.size(); }

    static bool empty(Vector& self) { return self.empty(); }

    static void bind(ForeignModule& m, const std::string& name) {
        auto& iter = m.klass<Iterator>(name + "Iter");
        iter.ctor();

        auto& cls = m.klass<Vector>(name);
        cls.ctor();

        cls.template funcExt<&StdVectorBindings<T>::getIndex>(OPERATOR_GET_INDEX);
        cls.template funcExt<&StdVectorBindings<T>::setIndex>(OPERATOR_SET_INDEX);
        cls.template funcExt<&StdVectorBindings<T>::add>("add");
        cls.template funcExt<&StdVectorBindings<T>::iterate>("iterate");
        cls.template funcExt<&StdVectorBindings<T>::iteratorValue>("iteratorValue");
        cls.template funcExt<&StdVectorBindings<T>::removeAt>("removeAt");
        cls.template funcExt<&StdVectorBindings<T>::insert>("insert");
        cls.template funcExt<&StdVectorBindings<T>::contains>("contains");
        cls.template funcExt<&StdVectorBindings<T>::pop>("pop");
        cls.template funcExt<&StdVectorBindings<T>::clear>("clear");
        cls.template funcExt<&StdVectorBindings<T>::size>("size");
        cls.template funcExt<&StdVectorBindings<T>::empty>("empty");
        cls.template propReadonlyExt<&StdVectorBindings<T>::count>("count");
    }
};

template <typename T>
class StdListBindings {
public:
    typedef typename std::list<T>::iterator Iterator;
    typedef typename std::list<T> List;

    static void setIndex(List& self, size_t index, T value) {
        auto it = self.begin();
        std::advance(it, index);
        *it = std::move(value);
    }

    static const T& getIndex(List& self, size_t index) {
        auto it = self.begin();
        std::advance(it, index);
        return *it;
    }

    static void add(List& self, T value) { self.push_back(std::move(value)); }

    static std::variant<bool, Iterator> iterate(List& self, std::variant<std::nullptr_t, Iterator> other) {
        if (other.index() == 1) {
            auto it = std::get<Iterator>(other);
            ++it;
            if (it != self.end()) {
                return {it};
            }

            return {false};
        } else {
            if (self.empty()) return {false};
            return {self.begin()};
        }
    }

    static const T& iteratorValue(List& self, std::shared_ptr<Iterator> other) {
        auto& it = *other;
        return *it;
    }

    static size_t count(List& self) { return self.size(); }

    static T removeAt(List& self, int32_t index) {
        if (index == -1) {
            auto ret = std::move(self.back());
            self.pop_back();
            return std::move(ret);
        } else {
            if (index < 0) {
                index = static_cast<int32_t>(self.size()) + index;
            }

            if (index > static_cast<int32_t>(self.size())) {
                throw std::out_of_range("invalid index");
            } else if (index == static_cast<int32_t>(self.size())) {
                auto ret = std::move(self.back());
                self.pop_back();
                return std::move(ret);
            } else {
                auto it = self.begin();
                std::advance(it, index);
                auto ret = std::move(*it);
                self.erase(it);
                return std::move(ret);
            }
        }
    }

    static void insert(List& self, int32_t index, T value) {
        if (index == -1) {
            self.push_back(std::move(value));
        } else {
            if (index < 0) {
                index = static_cast<int32_t>(self.size()) + index;
            }

            if (index > static_cast<int32_t>(self.size())) {
                throw std::out_of_range("invalid index");
            } else if (index == static_cast<int32_t>(self.size())) {
                self.push_back(std::move(value));
            } else {
                auto it = self.begin();
                std::advance(it, index);
                self.insert(it, std::move(value));
            }
        }
    }

    static bool contains(List& self, const T& value) {
        return std::find(self.begin(), self.end(), value) != self.end();
    }

    static T pop(List& self) {
        auto ret = std::move(self.back());
        self.pop_back();
        return std::move(ret);
    }

    static void clear(List& self) { self.clear(); }

    static size_t size(List& self) { return self.size(); }

    static bool empty(List& self) { return self.empty(); }

    static void bind(ForeignModule& m, const std::string& name) {
        auto& iter = m.klass<Iterator>(name + "Iter");
        iter.ctor();

        auto& cls = m.klass<List>(name);
        cls.ctor();

        cls.template funcExt<&StdListBindings<T>::getIndex>(OPERATOR_GET_INDEX);
        cls.template funcExt<&StdListBindings<T>::setIndex>(OPERATOR_SET_INDEX);
        cls.template funcExt<&StdListBindings<T>::add>("add");
        cls.template funcExt<&StdListBindings<T>::iterate>("iterate");
        cls.template funcExt<&StdListBindings<T>::iteratorValue>("iteratorValue");
        cls.template funcExt<&StdListBindings<T>::removeAt>("removeAt");
        cls.template funcExt<&StdListBindings<T>::insert>("insert");
        cls.template funcExt<&StdListBindings<T>::contains>("contains");
        cls.template funcExt<&StdListBindings<T>::pop>("pop");
        cls.template funcExt<&StdListBindings<T>::clear>("clear");
        cls.template funcExt<&StdListBindings<T>::size>("size");
        cls.template funcExt<&StdListBindings<T>::empty>("empty");
        cls.template propReadonlyExt<&StdListBindings<T>::count>("count");
    }
};

template <typename Map>
class AbstractMapBindings {
public:
    typedef typename Map::key_type K;
    typedef typename Map::mapped_type T;
    typedef typename Map::iterator Iterator;
    typedef typename Map::value_type Pair;

    static void setIndex(Map& self, const K& key, T value) { self[key] = std::move(value); }

    static T& getIndex(Map& self, const K& key) { return self[key]; }

    static std::variant<T, std::nullptr_t> remove(Map& self, const K& key) {
        auto it = self.find(key);
        if (it != self.end()) {
            auto ret = std::move(it->second);
            self.erase(it);
            return {ret};
        } else {
            return {nullptr};
        }
    }

    static bool containsKey(Map& self, const K& key) { return self.find(key) != self.end(); }

    static size_t count(Map& self) { return self.size(); }

    static void clear(Map& self) { self.clear(); }

    static size_t size(Map& self) { return self.size(); }

    static bool empty(Map& self) { return self.empty(); }

    static std::variant<bool, Iterator> iterate(Map& self, std::variant<std::nullptr_t, Iterator> other) {
        if (other.index() == 1) {
            auto it = std::get<Iterator>(other);
            ++it;
            if (it != self.end()) {
                return {it};
            }

            return {false};
        } else {
            return {self.begin()};
        }
    }

    static Pair iteratorValue(Map& self, std::shared_ptr<Iterator> other) {
        auto& it = *other;
        return *it;
    }

    static const K& pairKey(Pair& pair) { return pair.first; }

    static const T& pairValue(Pair& pair) { return pair.second; }

    static void bind(ForeignModule& m, const std::string& name) {
        auto& pair = m.klass<Pair>(name + "Pair");
        pair.template propReadonlyExt<&AbstractMapBindings<Map>::pairKey>("key");
        pair.template propReadonlyExt<&AbstractMapBindings<Map>::pairValue>("value");

        auto& iter = m.klass<Iterator>(name + "Iter");
        iter.ctor();

        auto& cls = m.klass<Map>(name);
        cls.ctor();

        cls.template funcExt<&AbstractMapBindings<Map>::getIndex>(OPERATOR_GET_INDEX);
        cls.template funcExt<&AbstractMapBindings<Map>::setIndex>(OPERATOR_SET_INDEX);
        cls.template funcExt<&AbstractMapBindings<Map>::remove>("remove");
        cls.template funcExt<&AbstractMapBindings<Map>::containsKey>("containsKey");
        cls.template funcExt<&AbstractMapBindings<Map>::iterate>("iterate");
        cls.template funcExt<&AbstractMapBindings<Map>::iteratorValue>("iteratorValue");
        cls.template funcExt<&AbstractMapBindings<Map>::clear>("clear");
        cls.template funcExt<&AbstractMapBindings<Map>::size>("size");
        cls.template funcExt<&AbstractMapBindings<Map>::empty>("empty");
        cls.template propReadonlyExt<&AbstractMapBindings<Map>::count>("count");
    }
};

template <typename K, typename V>
using StdMapBindings = AbstractMapBindings<std::map<K, V>>;

template <typename K, typename V>
using StdUnorderedMapBindings = AbstractMapBindings<std::unordered_map<K, V>>;
} // namespace wrenbind17
