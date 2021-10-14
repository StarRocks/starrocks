// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/rest_monitor_iface.h

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

#ifndef STARROCKS_BE_UTIL_REST_MONITOR_IFACE_H
#define STARROCKS_BE_UTIL_REST_MONITOR_IFACE_H

#include <sstream>

namespace starrocks {

// This is a interface used to monitor internal module running state.
class RestMonitorIface {
public:
    virtual ~RestMonitorIface() = default;

    // this is called when client want to know it's content
    virtual void debug(std::stringstream& ss) = 0;
};

} // namespace starrocks

#endif
