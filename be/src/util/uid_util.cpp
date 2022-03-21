// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/uid_util.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/uid_util.h"

#include "gutil/endian.h"
#include "util/uuid_generator.h"

namespace starrocks {

std::ostream& operator<<(std::ostream& os, const UniqueId& uid) {
    os << uid.to_string();
    return os;
}

std::string print_id(const TUniqueId& id) {
    boost::uuids::uuid uuid{};
    int64_t hi = gbswap_64(id.hi);
    int64_t lo = gbswap_64(id.lo);
    memcpy(uuid.data + 0, &hi, 8);
    memcpy(uuid.data + 8, &lo, 8);
    return boost::uuids::to_string(uuid);
}

std::string print_id(const PUniqueId& id) {
    boost::uuids::uuid uuid{};
    int64_t lo = gbswap_64(id.lo());
    int64_t hi = gbswap_64(id.hi());
    memcpy(uuid.data + 0, &hi, 8);
    memcpy(uuid.data + 8, &lo, 8);
    return boost::uuids::to_string(uuid);
}

UniqueId UniqueId::gen_uid() {
    UniqueId uid(0, 0);
    auto uuid = UUIDGenerator::instance()->next_uuid();
    memcpy(&uid.hi, uuid.data, sizeof(int64_t));
    memcpy(&uid.lo, uuid.data + sizeof(int64_t), sizeof(int64_t));
    return uid;
}

/// generates a 16 byte
std::string generate_uuid_string() {
    return boost::uuids::to_string(boost::uuids::basic_random_generator<boost::mt19937>()());
}

/// generates a 16 byte UUID
TUniqueId generate_uuid() {
    auto uuid = boost::uuids::basic_random_generator<boost::mt19937>()();
    TUniqueId uid;
    memcpy(&uid.hi, uuid.data, sizeof(int64_t));
    memcpy(&uid.lo, uuid.data + sizeof(int64_t), sizeof(int64_t));
    return uid;
}

} // namespace starrocks
