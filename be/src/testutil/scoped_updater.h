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

namespace starrocks {

template <typename T>
class ScopedUpdater {
public:
    ScopedUpdater(T& param, T value) : _param(param), _saved_old_value(param) { param = value; }
    ~ScopedUpdater() { _param = _saved_old_value; }

private:
    T& _param;
    T _saved_old_value;
};

#define CONCAT(x, y) x##y
#define CONCAT2(x, y) CONCAT(x, y)
#define SCOPED_UPDATE(type, param, value) \
    std::shared_ptr<ScopedUpdater<type>> CONCAT2(_var_updater_, __LINE__)(new ScopedUpdater<type>(param, value))

} // namespace starrocks
