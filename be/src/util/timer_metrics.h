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

#include <sstream>

namespace starrocks {
class TimerMetrics {
public:
    static TimerMetrics* instance() {
        static TimerMetrics instance;
        return &instance;
    }

    explicit TimerMetrics() = default;
    ~TimerMetrics() = default;
    std::string timer_result;
    std::string doTimerMetrics();

private:
    std::string printScanTabletBytes();
    std::string printScanTabletNum();
    std::string formatOutput(const std::string& name, const std::string& labelName, long label, long value);
    std::string formatOutput(const std::string& name, const std::string& labelName, const std::string& label,
                             long value);
};
} // namespace starrocks