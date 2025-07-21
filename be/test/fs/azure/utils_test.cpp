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

#include "fs/azure/utils.h"

#include <gtest/gtest.h>

namespace starrocks {

using namespace Azure::Core::Http;

class AzureUtilsTest : public ::testing::Test {};

TEST_F(AzureUtilsTest, test_azure_error_to_status) {
    std::string message = "error";
    std::string object = "abc";
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::None, message, object).is_unknown());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::Continue, "", "").ok());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::Ok, "", "").ok());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::MultipleChoices, message, object).is_aborted());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::BadRequest, message, object).is_invalid_argument());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::Unauthorized, message, object).is_not_authorized());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::PaymentRequired, message, object).is_invalid_argument());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::Forbidden, message, object).is_not_authorized());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::NotFound, message, object).is_not_found());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::MethodNotAllowed, message, object).is_invalid_argument());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::RequestTimeout, message, object).is_time_out());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::Conflict, message, object).is_internal_error());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::UriTooLong, message, object).is_invalid_argument());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::TooManyRequests, message, object).is_resource_busy());
    EXPECT_TRUE(
            azure_error_to_status(HttpStatusCode::RequestHeaderFieldsTooLarge, message, object).is_invalid_argument());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::InternalServerError, message, object).is_internal_error());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::NotImplemented, message, object).is_not_supported());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::BadGateway, message, object).is_internal_error());
    EXPECT_TRUE(azure_error_to_status(HttpStatusCode::ServiceUnavailable, message, object).is_service_unavailable());
    EXPECT_TRUE(
            azure_error_to_status(HttpStatusCode::NetworkAuthenticationRequired, message, object).is_internal_error());
}

} // namespace starrocks
