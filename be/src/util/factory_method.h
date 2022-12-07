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
