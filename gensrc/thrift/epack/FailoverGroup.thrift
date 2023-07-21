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

enum TFailoverGroupState {
    INITIALIZING,
    RUNNING,
    REPLICATING,
    ERROR
}

struct TFailoverGroupMember {
    1: required string name
    2: required set<Types.TNetworkAddress> addresses
    3: required Types.TNetworkAddress leader
    4: required TFailoverGroupRole role
    5: required TFailoverGroupState state
}

struct TFailoverGroupHandshakeRequest {
    1: required string failover_group_name
    2: required TFailoverGroupMember primary_member
    3: required binary failover_group_meta
}

struct TFailoverGroupHandshakeResponse {
    1: required Status.TStatus status 
}

struct TFailoverGroupRequestMetaRequest {
    1: required string failover_group_name
    2: required TFailoverGroupMember secondary_member
}

struct TFailoverGroupRequestMetaResponse {
    1: required Status.TStatus status
    2: optional binary replicated_object_meta
}