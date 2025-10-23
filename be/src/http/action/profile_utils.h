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

namespace starrocks {

// Utility functions for profile file handling
class ProfileUtils {
public:
    // Extracts timestamp from the profile filename.
    // Supports filename patterns such as:
    //   - cpu-profile-20231201-143022-flame.html.gz
    //   - contention-profile-20231201-143022-pprof.gz
    // Returns the extracted timestamp in the format "YYYYMMDD-HHmmss",
    // or an empty string if not found. The input is the profile filename.
    static std::string extract_timestamp_from_filename(const std::string& filename);

    // Determines the profile type from the filename.
    //
    // @param filename The profile filename.
    // @return Profile type: "CPU", "Contention", or "Unknown".
    static std::string get_profile_type(const std::string& filename);

    // Determines the profile format from the filename.
    //
    // @param filename The profile filename.
    // @return Returns "Flame", "Pprof", or "Unknown" based on the filename.
    static std::string get_profile_format(const std::string& filename);
};

} // namespace starrocks
