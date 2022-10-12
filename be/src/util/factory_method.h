// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>

namespace starrocks {
template <typename Base, typename Derived>
class FactoryMethod : public Base {
public:
    template <typename... Args>
    FactoryMethod(Args&&... args) : Base(std::forward<Args>(args)...) {}

    using DerivedPtr = std::shared_ptr<Derived>;
    template <typename... Args>
    static DerivedPtr create(Args&&... args) {
        return std::make_shared<Derived>(std::forward<Args>(args)...);
    }

private:
    using Self = FactoryMethod<Base, Derived>;
    Self& operator=(const Self&) = delete;
};
} // namespace starrocks
