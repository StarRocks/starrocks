// Copyright 2021-present StarRocks, Inc. All rights reserved.

namespace cpp starrocks
namespace java com.starrocks.epack.thrift

include "../Status.thrift"
include "../Types.thrift"

enum TFailoverGroupRole {
    NONE,
    PRIMARY,
    SECONDARY
}

struct TFailoverGroupMember {
    1: optional string name
    2: optional set<Types.TNetworkAddress> addresses
    3: optional Types.TNetworkAddress leader
    4: optional TFailoverGroupRole role
}

struct TFailoverGroupHandshakeRequest {
    1: optional string failover_group_name
    2: optional TFailoverGroupMember primary_member
    3: optional binary failover_group_meta
}

struct TFailoverGroupHandshakeResponse {
    1: optional Status.TStatus status 
}

struct TFailoverGroupRequestMetaRequest {
    1: optional string failover_group_name
    2: optional TFailoverGroupMember secondary_member
    3: optional i64 last_meta_version
}

struct TFailoverGroupRequestMetaResponse {
    1: optional Status.TStatus status
    2: optional i64 meta_version
    3: optional string primary_token
    4: optional i32 primary_http_port
}