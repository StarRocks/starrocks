#include "http/action/profile_utils.h"

#include <cctype>
#include <regex>

namespace starrocks {

std::string ProfileUtils::extract_timestamp_from_filename(const std::string& filename) {
    // Extract timestamp from filename like: cpu-profile-20231201-143022-flame.html.gz
    // or: cpu-profile-20231201-143022-pprof.gz

    // Regex pattern to match: profile-type-YYYYMMDD-HHmmss-format.gz
    // Group 1: profile type (cpu-profile or contention-profile)
    // Group 2: date (YYYYMMDD)
    // Group 3: time (HHmmss)
    // Group 4: format (flame.html or pprof)
    std::regex pattern(R"((cpu-profile|contention-profile)-(\d{8})-(\d{6})-(flame\.html|pprof)\.gz)");

    std::smatch matches;
    if (std::regex_match(filename, matches, pattern)) {
        if (matches.size() >= 4) {
            std::string date = matches[2].str(); // YYYYMMDD
            std::string time = matches[3].str(); // HHmmss
            return date + "-" + time;
        }
    }

    // Fallback: try to match just the date part for older formats
    std::regex fallback_pattern(R"((cpu-profile|contention-profile)-(\d{8})-.*\.gz)");
    if (std::regex_match(filename, matches, fallback_pattern)) {
        if (matches.size() >= 3) {
            return matches[2].str(); // Just the date
        }
    }

    return "";
}

std::string ProfileUtils::get_profile_type(const std::string& filename) {
    if (filename.find("cpu-profile-") == 0) {
        return "CPU";
    } else if (filename.find("contention-profile-") == 0) {
        return "Contention";
    } else {
        return "Unknown";
    }
}

std::string ProfileUtils::get_profile_format(const std::string& filename) {
    if (filename.find("-flame.html.gz") != std::string::npos) {
        return "Flame";
    } else if (filename.find("-pprof.gz") != std::string::npos) {
        return "Pprof";
    } else {
        return "Unknown";
    }
}

} // namespace starrocks
