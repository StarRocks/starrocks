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

#include "base/auth/auth_info.h"

#include <gtest/gtest.h>

#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

// set_request_auth is the one template every load RPC fills its credential from. The two request
// shapes differ in what they declare: TLoadTxnBeginRequest has auth_code / cluster and required
// user / passwd; TMergeCommitRequest has neither auth_code nor cluster, and its user / passwd are
// optional - which is why the setters matter (a plain assignment leaves __isset false and the
// field off the wire, and the FE then sees a null password).

TEST(SetRequestAuthTest, merge_commit_request_carries_user_passwd_and_auth_type_on_the_wire) {
    AuthInfo auth;
    auth.user = "harbor";
    auth.passwd = "eyJ.hdr.sig";
    auth.user_ip = "10.1.2.3";
    auth.auth_type = AuthInfo::AUTH_TYPE_BEARER;

    TMergeCommitRequest req;
    set_request_auth(&req, auth);
    EXPECT_TRUE(req.__isset.user);
    EXPECT_TRUE(req.__isset.passwd);
    EXPECT_TRUE(req.__isset.user_ip);
    EXPECT_TRUE(req.__isset.auth_type);
    EXPECT_EQ("harbor", req.user);
    EXPECT_EQ("eyJ.hdr.sig", req.passwd);
    EXPECT_EQ("bearer", req.auth_type);
}

TEST(SetRequestAuthTest, basic_leaves_auth_type_unset_which_the_fe_reads_as_basic) {
    AuthInfo auth;
    auth.user = "alice";
    auth.passwd = "pw";
    auth.user_ip = "10.1.2.3";

    TMergeCommitRequest merge;
    set_request_auth(&merge, auth);
    EXPECT_TRUE(merge.__isset.passwd);
    EXPECT_EQ("pw", merge.passwd);
    EXPECT_FALSE(merge.__isset.auth_type) << "empty auth_type stays off the wire; unset == basic";

    TLoadTxnBeginRequest begin;
    set_request_auth(&begin, auth);
    EXPECT_EQ("alice", begin.user);
    EXPECT_EQ("pw", begin.passwd);
    EXPECT_FALSE(begin.__isset.auth_type);
    EXPECT_FALSE(begin.__isset.cluster) << "an empty cluster is not sent";
}

TEST(SetRequestAuthTest, txn_begin_request_gets_cluster_and_auth_type) {
    AuthInfo auth;
    auth.user = "alice";
    auth.passwd = "tok";
    auth.cluster = "c1";
    auth.auth_type = AuthInfo::AUTH_TYPE_BEARER;

    TLoadTxnBeginRequest begin;
    set_request_auth(&begin, auth);
    EXPECT_TRUE(begin.__isset.cluster);
    EXPECT_EQ("c1", begin.cluster);
    EXPECT_TRUE(begin.__isset.auth_type);
    EXPECT_EQ("bearer", begin.auth_type);
}

TEST(SetRequestAuthTest, auth_code_short_circuits_and_still_sets_the_required_fields) {
    AuthInfo auth;
    auth.user = "ignored";
    auth.passwd = "ignored";
    auth.auth_code = 42;
    auth.auth_type = AuthInfo::AUTH_TYPE_BEARER;

    TLoadTxnBeginRequest begin;
    set_request_auth(&begin, auth);
    EXPECT_TRUE(begin.__isset.auth_code);
    EXPECT_EQ(42, begin.auth_code);
    EXPECT_EQ("", begin.user);
    EXPECT_EQ("", begin.passwd);
    EXPECT_FALSE(begin.__isset.auth_type) << "an internal RPC carries no client credential kind";
}

TEST(SetRequestAuthTest, be_http_gate_authenticate_params_take_host_and_auth_type) {
    // The BE HTTP gate's TAuthenticateParams names the client address `host`, not `user_ip`, and has
    // neither auth_code nor cluster; the same template must still carry the Bearer token to the FE.
    AuthInfo auth;
    auth.user = "";
    auth.passwd = "eyJ.hdr.sig";
    auth.user_ip = "10.0.0.7";
    auth.auth_type = "bearer";
    TAuthenticateParams params;
    set_request_auth(&params, auth);
    EXPECT_EQ("", params.user);
    EXPECT_EQ("eyJ.hdr.sig", params.passwd);
    EXPECT_TRUE(params.__isset.host);
    EXPECT_EQ("10.0.0.7", params.host);
    EXPECT_TRUE(params.__isset.auth_type);
    EXPECT_EQ("bearer", params.auth_type);
}

} // namespace starrocks
