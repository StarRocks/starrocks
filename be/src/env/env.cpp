// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env.h"

#include <fmt/format.h>

#include <regex>
#include <type_traits>

#include "env/env_hdfs.h"
#include "env/env_posix.h"
#include "env/env_s3.h"
#include "gutil/macros.h"

namespace starrocks {

class EnvRegistry {
public:
    using FactoryFunc = Env::FactoryFunc;

    static EnvRegistry& Instance() {
        static EnvRegistry instance;
        return instance;
    }

    DISALLOW_COPY_AND_ASSIGN(EnvRegistry);

    void register_env(std::string_view pattern, FactoryFunc func) {
        _entries.emplace_back(Entry{std::regex(pattern.begin(), pattern.end()), std::move(func)});
    }

    auto create_env(std::string_view uri) -> std::invoke_result_t<FactoryFunc, std::string_view> {
        for (auto&& [pattern, func] : _entries) {
            if (std::regex_match(uri.begin(), uri.end(), pattern)) return func(uri);
        }
        return Status::NotFound(fmt::format("No registered Env for URI {}", uri));
    }

    auto create_env_or_default(std::string_view uri) -> std::invoke_result_t<FactoryFunc, std::string_view> {
        for (auto&& [pattern, func] : _entries) {
            if (std::regex_match(uri.begin(), uri.end(), pattern)) return func(uri);
        }
        return new_env_posix();
    }

private:
    EnvRegistry() = default;

    struct Entry {
        std::regex pattern;
        FactoryFunc func;
    };

    std::vector<Entry> _entries;
};

void Env::Register(std::string_view pattern, FactoryFunc func) {
    EnvRegistry::Instance().register_env(pattern, std::move(func));
}

StatusOr<std::unique_ptr<Env>> Env::CreateUniqueFromString(std::string_view uri) {
    return EnvRegistry::Instance().create_env(uri);
}

StatusOr<std::shared_ptr<Env>> Env::CreateSharedFromString(std::string_view uri) {
    ASSIGN_OR_RETURN(auto ptr, CreateUniqueFromString(uri));
    return std::shared_ptr<Env>(std::move(ptr));
}

StatusOr<std::unique_ptr<Env>> Env::CreateUniqueFromStringOrDefault(std::string_view uri) {
    return EnvRegistry::Instance().create_env_or_default(uri);
}

StatusOr<std::shared_ptr<Env>> Env::CreateSharedFromStringOrDefault(std::string_view uri) {
    ASSIGN_OR_RETURN(auto ptr, CreateUniqueFromStringOrDefault(uri));
    return std::shared_ptr<Env>(std::move(ptr));
}

class EnvGlobalInitializer {
public:
    EnvGlobalInitializer() {
        Env::Register("posix://.*", [](std::string_view /*uri*/) { return new_env_posix(); });
        Env::Register("hdfs://.*", [](std::string_view /*uri*/) { return std::make_unique<EnvHdfs>(); });
        Env::Register("oss://.*", [](std::string_view /*uri*/) { return std::make_unique<EnvS3>(); });
        Env::Register("s3a://.*", [](std::string_view /*uri*/) { return std::make_unique<EnvS3>(); });
        Env::Register("s3n://.*", [](std::string_view /*uri*/) { return std::make_unique<EnvS3>(); });
        Env::Register("s3://.*", [](std::string_view /*uri*/) { return std::make_unique<EnvS3>(); });
    }
};

static EnvGlobalInitializer obj;

} // namespace starrocks
