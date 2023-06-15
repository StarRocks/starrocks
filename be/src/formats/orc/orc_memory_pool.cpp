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

#include <glog/logging.h>

#include <orc/OrcFile.hh>

namespace starrocks {

class OrcMemoryPoolImpl : public orc::MemoryPool {
public:
    ~OrcMemoryPoolImpl() override = default;

    char* malloc(uint64_t size) override {
        auto p = static_cast<char*>(std::malloc(size));
        if (p == nullptr) {
            LOG(WARNING) << "malloc failed, size=" << size;
            throw std::bad_alloc();
        }
        return p;
    }

    void free(char* p) override { std::free(p); }
};

orc::MemoryPool* getOrcMemoryPool() {
    static OrcMemoryPoolImpl internal;
    return &internal;
}

} // namespace starrocks
