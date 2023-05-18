#pragma once

#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <wren.hpp>

#include "foreign.hpp"

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
void addClassType(WrenVM* vm, const std::string& module, const std::string& name, size_t hash);
void addClassCast(WrenVM* vm, std::shared_ptr<detail::ForeignPtrConvertor> convertor, size_t hash, size_t other);

/**
     * @ingroup wrenbind17
     */
class ForeignModule {
public:
    ForeignModule(std::string name, WrenVM* vm) : name(std::move(name)), vm(vm) {}
    ForeignModule(const ForeignModule& other) = delete;
    ForeignModule(ForeignModule&& other) noexcept : vm(nullptr) { swap(other); }
    ~ForeignModule() = default;
    ForeignModule& operator=(const ForeignModule& other) = delete;
    ForeignModule& operator=(ForeignModule&& other) noexcept {
        if (this != &other) {
            swap(other);
        }
        return *this;
    }
    void swap(ForeignModule& other) {
        std::swap(klasses, other.klasses);
        std::swap(vm, other.vm);
        std::swap(name, other.name);
    }

    template <typename T, typename... Others>
    ForeignKlassImpl<T>& klass(std::string name) {
        insertKlassCast<T, Others...>();
        auto ptr = std::make_unique<ForeignKlassImpl<T>>(std::move(name));
        auto ret = ptr.get();
        addClassType(vm, this->name, ptr->getName(), typeid(T).hash_code());
        klasses.insert(std::make_pair(ptr->getName(), std::move(ptr)));
        return *ret;
    }

    std::string str() const {
        std::stringstream ss;
        for (const auto& pair : klasses) {
            pair.second->generate(ss);
        }
        for (const auto& r : raw) {
            ss << r << "\n";
        }
        return ss.str();
    }

    void append(std::string text) { raw.push_back(std::move(text)); }

    ForeignKlass& findKlass(const std::string& name) {
        auto it = klasses.find(name);
        if (it == klasses.end()) throw NotFound();
        return *it->second;
    }

    const std::string& getName() const { return name; }

private:
    template <typename T>
    void insertKlassCast() {
        // void
    }

    template <typename T, typename Other, typename... Others>
    void insertKlassCast() {
        addClassCast(vm, std::make_shared<detail::ForeignObjectSharedPtrConvertor<T, Other>>(), typeid(T).hash_code(),
                     typeid(Other).hash_code());
        insertKlassCast<T, Others...>();
    }

    std::string name;
    WrenVM* vm;
    std::unordered_map<std::string, std::unique_ptr<ForeignKlass>> klasses;
    std::vector<std::string> raw;
};
} // namespace wrenbind17
