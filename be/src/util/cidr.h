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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/cidr.h

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

#pragma once

<<<<<<< HEAD
=======
#include <sys/un.h>

#include <array>
#include <cstdint>
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
#include <string>

namespace starrocks {

// Classless Inter-Domain Routing
class CIDR {
public:
    CIDR();
<<<<<<< HEAD
    void reset();
    bool reset(const std::string& cidr_str);
    bool contains(const std::string& ip);
    static bool ip_to_int(const std::string& ip_str, uint32_t* value);

private:
    bool contains(uint32_t ip_int);

    uint32_t _address{0};
    uint32_t _netmask{0xffffffff};
=======
    bool reset(const std::string& cidr_str);
    bool contains(const CIDR& ip) const;
    static bool ip_to_int(const std::string& ip_str, uint32_t* value);

private:
    sa_family_t _family;
    std::array<std::uint8_t, 16> _address;
    std::uint8_t _netmask_len;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
};

} // end namespace starrocks
