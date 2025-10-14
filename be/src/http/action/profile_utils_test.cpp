#include "http/action/profile_utils.h"

#include <gtest/gtest.h>

namespace starrocks {

class ProfileUtilsTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ProfileUtilsTest, ExtractTimestampFromFilename) {
    // Test valid CPU profile files
    EXPECT_EQ(ProfileUtils::extract_timestamp_from_filename("cpu-profile-20231201-143022-flame.html.gz"),
              "20231201-143022");
    EXPECT_EQ(ProfileUtils::extract_timestamp_from_filename("cpu-profile-20231201-143022-pprof.gz"), "20231201-143022");

    // Test valid contention profile files
    EXPECT_EQ(ProfileUtils::extract_timestamp_from_filename("contention-profile-20231201-143022-flame.html.gz"),
              "20231201-143022");
    EXPECT_EQ(ProfileUtils::extract_timestamp_from_filename("contention-profile-20231201-143022-pprof.gz"),
              "20231201-143022");

    // Test different date/time
    EXPECT_EQ(ProfileUtils::extract_timestamp_from_filename("cpu-profile-20251014-091530-flame.html.gz"),
              "20251014-091530");

    // Test invalid files
    EXPECT_EQ(ProfileUtils::extract_timestamp_from_filename("invalid-filename.gz"), "");
    EXPECT_EQ(ProfileUtils::extract_timestamp_from_filename(""), "");
}

TEST_F(ProfileUtilsTest, GetProfileType) {
    EXPECT_EQ(ProfileUtils::get_profile_type("cpu-profile-20231201-143022-flame.html.gz"), "CPU");
    EXPECT_EQ(ProfileUtils::get_profile_type("contention-profile-20231201-143022-pprof.gz"), "Contention");
    EXPECT_EQ(ProfileUtils::get_profile_type("unknown-profile-20231201-143022-flame.html.gz"), "Unknown");
    EXPECT_EQ(ProfileUtils::get_profile_type(""), "Unknown");
}

TEST_F(ProfileUtilsTest, GetProfileFormat) {
    EXPECT_EQ(ProfileUtils::get_profile_format("cpu-profile-20231201-143022-flame.html.gz"), "Flame");
    EXPECT_EQ(ProfileUtils::get_profile_format("cpu-profile-20231201-143022-pprof.gz"), "Pprof");
    EXPECT_EQ(ProfileUtils::get_profile_format("cpu-profile-20231201-143022-unknown.gz"), "Unknown");
    EXPECT_EQ(ProfileUtils::get_profile_format(""), "Unknown");
}

} // namespace starrocks
