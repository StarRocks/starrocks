#pragma once

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <wren.hpp>

#include "exception.hpp"
#include "map.hpp"
#include "module.hpp"
#include "variable.hpp"

#ifndef DOXYGEN_SHOULD_SKIP_THIS
namespace std {
template <>
struct hash<std::pair<size_t, size_t>> {
    inline size_t operator()(const std::pair<size_t, size_t>& v) const {
        const std::hash<size_t> hasher;
        return hasher(v.first) ^ hasher(v.second);
    }
};
} // namespace std
#endif

/**
 * @ingroup wrenbind17
 */
namespace wrenbind17 {
/**
     * @ingroup wrenbind17
     */
typedef std::function<void(const char*)> PrintFn;

/**
     * @ingroup wrenbind17
     */
typedef std::function<std::string(const std::vector<std::string>& paths, const std::string& name)> LoadFileFn;

/**
     * @ingroup wrenbind17
     * @brief Holds the entire Wren VM from which all of the magic happens
     */
class VM {
public:
    /*!
         * @brief The only constructor available
         * @param paths The lookup paths used by the import loader function
         * @param initHeap The size of the heap at the beginning
         * @param minHeap The minimum size of the heap
         * @param heapGrowth How the heap should grow
         */
    inline explicit VM(std::vector<std::string> paths = {"./"}, const size_t initHeap = 1024 * 1024,
                       const size_t minHeap = 1024 * 1024 * 10, const int heapGrowth = 50)
            : data(std::make_unique<Data>()) {
        data->paths = std::move(paths);
        data->printFn = [](const char* text) -> void { std::cout << text; };
        data->loadFileFn = [](const std::vector<std::string>& paths, const std::string& name) -> std::string {
            for (const auto& path : paths) {
                const auto test = path + "/" + std::string(name) + ".wren";

                std::ifstream t(test);
                if (!t) continue;

                std::string source((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
                return source;
            }

            throw NotFound();
        };

        wrenInitConfiguration(&data->config);
        data->config.initialHeapSize = initHeap;
        data->config.minHeapSize = minHeap;
        data->config.heapGrowthPercent = heapGrowth;
        data->config.userData = data.get();
#if WREN_VERSION_NUMBER >= 4000 // >= 0.4.0
        data->config.reallocateFn = [](void* memory, size_t newSize, void* userData) -> void* {
            return std::realloc(memory, newSize);
        };
        data->config.loadModuleFn = [](WrenVM* vm, const char* name) -> WrenLoadModuleResult {
            auto res = WrenLoadModuleResult();
            auto& self = *reinterpret_cast<VM::Data*>(wrenGetUserData(vm));

            const auto mod = self.modules.find(name);
            if (mod != self.modules.end()) {
                auto source = mod->second.str();
                auto buffer = new char[source.size() + 1];
                std::memcpy(buffer, &source[0], source.size() + 1);
                res.source = buffer;
                res.onComplete = [](WrenVM* vm, const char* name, struct WrenLoadModuleResult result) {
                    delete[] result.source;
                };
                return res;
            }

            try {
                auto source = self.loadFileFn(self.paths, std::string(name));
                auto buffer = new char[source.size() + 1];
                std::memcpy(buffer, &source[0], source.size() + 1);
                res.source = buffer;
                res.onComplete = [](WrenVM* vm, const char* name, struct WrenLoadModuleResult result) {
                    delete[] result.source;
                };
            } catch (std::exception& e) {
                (void)e;
            }
            return res;
        };
#else  // < 0.4.0
        data->config.reallocateFn = std::realloc;
        data->config.loadModuleFn = [](WrenVM* vm, const char* name) -> char* {
            auto& self = *reinterpret_cast<VM::Data*>(wrenGetUserData(vm));

            const auto mod = self.modules.find(name);
            if (mod != self.modules.end()) {
                auto source = mod->second.str();
                auto buffer = new char[source.size() + 1];
                std::memcpy(buffer, &source[0], source.size() + 1);
                return buffer;
            }

            try {
                auto source = self.loadFileFn(self.paths, std::string(name));
                auto buffer = new char[source.size() + 1];
                std::memcpy(buffer, &source[0], source.size() + 1);
                return buffer;
            } catch (std::exception& e) {
                (void)e;
                return nullptr;
            }
        };
#endif // WREN_VERSION_NUMBER >= 4000
        data->config.bindForeignMethodFn = [](WrenVM* vm, const char* module, const char* className,
                                              const bool isStatic, const char* signature) -> WrenForeignMethodFn {
            auto& self = *reinterpret_cast<VM::Data*>(wrenGetUserData(vm));
            try {
                auto& found = self.modules.at(module);
                auto& klass = found.findKlass(className);
                return klass.findSignature(signature, isStatic);
            } catch (...) {
                std::cerr << "Wren foreign method " << signature << " not found in C++" << std::endl;
                std::abort();
                return nullptr;
            }
        };
        data->config.bindForeignClassFn = [](WrenVM* vm, const char* module,
                                             const char* className) -> WrenForeignClassMethods {
            auto& self = *reinterpret_cast<VM::Data*>(wrenGetUserData(vm));
            try {
                auto& found = self.modules.at(module);
                auto& klass = found.findKlass(className);
                return klass.getAllocators();
            } catch (...) {
                exceptionHandler(vm, std::current_exception());
                return WrenForeignClassMethods{nullptr, nullptr};
            }
        };
        data->config.writeFn = [](WrenVM* vm, const char* text) {
            auto& self = *reinterpret_cast<VM::Data*>(wrenGetUserData(vm));
            self.printFn(text);
        };
        data->config.errorFn = [](WrenVM* vm, WrenErrorType type, const char* module, const int line,
                                  const char* message) {
            auto& self = *reinterpret_cast<VM::Data*>(wrenGetUserData(vm));
            std::stringstream ss;
            switch (type) {
            case WREN_ERROR_COMPILE:
                ss << "Compile error: " << message << " at " << module << ":" << line << "\n";
                break;
            case WREN_ERROR_RUNTIME:
                if (!self.nextError.empty()) {
                    ss << "Runtime error: " << self.nextError << "\n";
                    self.nextError.clear();
                } else {
                    ss << "Runtime error: " << message << "\n";
                }
                break;
            case WREN_ERROR_STACK_TRACE:
                ss << "  at: " << module << ":" << line << "\n";
                break;
            default:
                break;
            }
            self.lastError += ss.str();
        };

        data->vm = std::shared_ptr<WrenVM>(wrenNewVM(&data->config), [](WrenVM* ptr) { wrenFreeVM(ptr); });
    }

    inline VM(const VM& other) = delete;

    inline VM(VM&& other) noexcept { swap(other); }

    inline ~VM() = default;

    inline VM& operator=(const VM& other) = delete;

    inline VM& operator=(VM&& other) noexcept {
        if (this != &other) {
            swap(other);
        }
        return *this;
    }

    inline void swap(VM& other) noexcept { std::swap(data, other.data); }

    /*!
         * @brief Runs a Wren source code by passing it as a string
         * @param name The module name to assign this code into, this module name
         * can be then used to import this code in some other place
         * @param code Your raw multiline Wren code
         * @throws CompileError if the compilation has failed
         */
    inline void runFromSource(const std::string& name, const std::string& code) {
        const auto result = wrenInterpret(data->vm.get(), name.c_str(), code.c_str());
        if (result != WREN_RESULT_SUCCESS) {
            throw CompileError(getLastError());
        }
        return;
    }

    /*!
         * @brief Runs a Wren source code directly from a file
         * @param name The module name to assign this code into, this module name
         * can be then used to import this code in some other place
         * @param path The path to the file
         * @throws Exception if the file has not been found or the file cannot be read
         * @throws CompileError if the compilation has failed
         */
    inline void runFromFile(const std::string& name, const std::string& path) {
        std::ifstream t(path);
        if (!t) throw Exception("Compile error: Failed to open source file");
        std::string str((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
        runFromSource(name, str);
    }

    /*!
         * @brief Runs a Wren source code by passing it as a string
         * @see setLoadFileFunc
         * @param name The module name to load, this will use the loader function to
         * load the file from
         * @throws CompileError if the compilation has failed
         */
    inline void runFromModule(const std::string& name) {
        const auto source = data->loadFileFn(data->paths, name);
        runFromSource(name, source);
    }

    /*!
         * @brief Looks up a variable from a module
         * @param module The name of the module to look for the variable in
         * @param name The name of the variable or a class itself that must start
         * with a capital letter
         * @throws NotFound if the variable has not been found
         */
    inline Variable find(const std::string& module, const std::string& name) {
        wrenEnsureSlots(data->vm.get(), 1);
        wrenGetVariable(data->vm.get(), module.c_str(), name.c_str(), 0);
        auto* handle = wrenGetSlotHandle(data->vm.get(), 0);
        if (!handle) throw NotFound();
        return Variable(std::make_shared<Handle>(data->vm, handle));
    }

    /*!
         * @brief Creates a new custom module
         * @note Calling this function multiple times with the same name
         * does not create a new module, but instead it returns the same module.
         */
    inline ForeignModule& module(const std::string& name) {
        auto it = data->modules.find(name);
        if (it == data->modules.end()) {
            it = data->modules.insert(std::make_pair(name, ForeignModule(name, data->vm.get()))).first;
        }
        return it->second;
    }

    inline void addClassType(const std::string& module, const std::string& name, const size_t hash) {
        data->addClassType(module, name, hash);
    }

    inline void getClassType(std::string& module, std::string& name, const size_t hash) {
        data->getClassType(module, name, hash);
    }

    inline bool isClassRegistered(const size_t hash) const { return data->isClassRegistered(hash); }

    inline void addClassCast(std::shared_ptr<detail::ForeignPtrConvertor> convertor, const size_t hash,
                             const size_t other) {
        data->addClassCast(std::move(convertor), hash, other);
    }

    inline detail::ForeignPtrConvertor* getClassCast(const size_t hash, const size_t other) {
        return data->getClassCast(hash, other);
    }

    inline std::string getLastError() { return data->getLastError(); }

    inline void setNextError(std::string str) { data->setNextError(std::move(str)); }

    /*!
         * @brief Set a custom print function that is used by the System.print()
         * @see PrintFn
         */
    inline void setPrintFunc(const PrintFn& fn) { data->printFn = fn; }

    /*!
         * @brief Set a custom loader function for imports
         * @see LoadFileFn
         * @details This must be a function that accepts a std::vector of strings
         * (which are the lookup paths from the constructor) and the name of the import
         * as the second parameter. You must return the source code from this custom function.
         * If you want to cancel the import, simply throw an exception.
         */
    inline void setLoadFileFunc(const LoadFileFn& fn) { data->loadFileFn = fn; }

    /*!
         * @brief Runs the garbage collector
         */
    inline void gc() { wrenCollectGarbage(data->vm.get()); }

    class Data {
    public:
        std::shared_ptr<WrenVM> vm;
        WrenConfiguration config;
        std::vector<std::string> paths;
        std::unordered_map<std::string, ForeignModule> modules;
        std::unordered_map<size_t, std::string> classToModule;
        std::unordered_map<size_t, std::string> classToName;
        std::unordered_map<std::pair<size_t, size_t>, std::shared_ptr<detail::ForeignPtrConvertor>> classCasting;
        std::string lastError;
        std::string nextError;
        PrintFn printFn;
        LoadFileFn loadFileFn;

        inline void addClassType(const std::string& module, const std::string& name, const size_t hash) {
            classToModule.insert(std::make_pair(hash, module));
            classToName.insert(std::make_pair(hash, name));
        }

        inline void getClassType(std::string& module, std::string& name, const size_t hash) {
            module = classToModule.at(hash);
            name = classToName.at(hash);
        }

        inline bool isClassRegistered(const size_t hash) const {
            return classToModule.find(hash) != classToModule.end();
        }

        inline void addClassCast(std::shared_ptr<detail::ForeignPtrConvertor> convertor, const size_t hash,
                                 const size_t other) {
            classCasting.insert(std::make_pair(std::make_pair(hash, other), std::move(convertor)));
        }

        inline detail::ForeignPtrConvertor* getClassCast(const size_t hash, const size_t other) {
            return classCasting.at(std::pair(hash, other)).get();
        }

        inline std::string getLastError() {
            std::string str;
            std::swap(str, lastError);
            return str;
        }

        inline void setNextError(std::string str) { nextError = std::move(str); }
    };

private:
    std::unique_ptr<Data> data;
};

#ifndef DOXYGEN_SHOULD_SKIP_THIS
inline std::shared_ptr<WrenVM> getSharedVm(WrenVM* vm) {
    assert(vm);
    auto self = reinterpret_cast<VM::Data*>(wrenGetUserData(vm));
    assert(self->vm);
    return self->vm;
}
inline void addClassType(WrenVM* vm, const std::string& module, const std::string& name, const size_t hash) {
    assert(vm);
    auto self = reinterpret_cast<VM::Data*>(wrenGetUserData(vm));
    self->addClassType(module, name, hash);
}
inline void getClassType(WrenVM* vm, std::string& module, std::string& name, const size_t hash) {
    assert(vm);
    auto self = reinterpret_cast<VM::Data*>(wrenGetUserData(vm));
    self->getClassType(module, name, hash);
}
inline bool isClassRegistered(WrenVM* vm, const size_t hash) {
    assert(vm);
    auto self = reinterpret_cast<VM::Data*>(wrenGetUserData(vm));
    return self->isClassRegistered(hash);
}
inline void addClassCast(WrenVM* vm, std::shared_ptr<detail::ForeignPtrConvertor> convertor, const size_t hash,
                         const size_t other) {
    assert(vm);
    auto self = reinterpret_cast<VM::Data*>(wrenGetUserData(vm));
    self->addClassCast(std::move(convertor), hash, other);
}
inline detail::ForeignPtrConvertor* getClassCast(WrenVM* vm, const size_t hash, const size_t other) {
    assert(vm);
    auto self = reinterpret_cast<VM::Data*>(wrenGetUserData(vm));
    return self->getClassCast(hash, other);
}
inline std::string getLastError(WrenVM* vm) {
    assert(vm);
    auto self = reinterpret_cast<VM::Data*>(wrenGetUserData(vm));
    return self->getLastError();
}
inline void setNextError(WrenVM* vm, std::string str) {
    assert(vm);
    auto self = reinterpret_cast<VM::Data*>(wrenGetUserData(vm));
    self->setNextError(std::move(str));
}
#endif
} // namespace wrenbind17
