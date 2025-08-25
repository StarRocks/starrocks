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

#include <string>
#include <unordered_map>
#include <unordered_set>

namespace starrocks {

// Runtime profile metric tier for readability improvements
enum ProfileMetricTier {
    BASIC_TIER = 0,    // Most critical metrics (≤5 per operator)
    ADVANCED_TIER = 1, // Additional performance metrics
    TRACE_TIER = 2     // Developer and debugging metrics
};

// Helper class for metric categorization and display filtering
class RuntimeProfileMetricHelper {
public:
    // Check if metric should be in basic tier (most important metrics)
    static bool is_basic_metric(const std::string& metric_name);
    
    // Check if metric should be in trace tier (developer/debug metrics)
    static bool is_trace_metric(const std::string& metric_name);
    
    // Get user-friendly display name for metric
    static std::string get_friendly_name(const std::string& metric_name);
    
    // Check if metric should always be displayed even when zero
    static bool always_display_when_zero(const std::string& metric_name);

private:
    static const std::unordered_set<std::string> BASIC_METRICS;
    static const std::unordered_set<std::string> TRACE_METRICS;  
    static const std::unordered_map<std::string, std::string> FRIENDLY_NAMES;
    static const std::unordered_set<std::string> ALWAYS_DISPLAY_ZERO;
    
    // Initialize static data
    static void initialize_if_needed();
    static bool initialized;
};

} // namespace starrocks