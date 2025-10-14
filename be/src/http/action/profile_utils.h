#pragma once

#include <string>

namespace starrocks {

/**
 * Utility functions for profile file handling
 */
class ProfileUtils {
public:
    /**
     * Extract timestamp from profile filename
     * 
     * Supports filename patterns like:
     * - cpu-profile-20231201-143022-flame.html.tar.gz
     * - contention-profile-20231201-143022-pprof.tar.gz
     * 
     * @param filename The profile filename
     * @return Extracted timestamp in format "YYYYMMDD-HHmmss" or empty string if not found
     */
    static std::string extract_timestamp_from_filename(const std::string& filename);

    /**
     * Determine profile type from filename
     * 
     * @param filename The profile filename
     * @return Profile type: "CPU", "Contention", or "Unknown"
     */
    static std::string get_profile_type(const std::string& filename);

    /**
     * Determine profile format from filename
     * 
     * @param filename The profile filename
     * @return Profile format: "Flame", "Pprof", or "Unknown"
     */
    static std::string get_profile_format(const std::string& filename);
};

} // namespace starrocks
