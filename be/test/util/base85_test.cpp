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

#include "util/base85.h"

#include <gtest/gtest.h>

#include "util/uuid_generator.h"

namespace starrocks {

class Base85Test : public testing::Test {};

TEST_F(Base85Test, decodeTest) {
    std::string encode = "bfX+S@(X0DVnaD&Lvg?b";
    auto decode_status = base85_decode(encode);
    auto decode = decode_status.value();
    boost::uuids::uuid uuid{};
    memcpy(uuid.data + 0, decode.data(), 8);
    memcpy(uuid.data + 8, decode.data() + 8, 8);
    ASSERT_EQ("22ccda6c-fecb-4cba-b232-3a299360b660", boost::uuids::to_string(uuid));
}

} // namespace starrocks