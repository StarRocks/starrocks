// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.alter;

// DecommissionType is for compatible with older versions of metadata
public enum DecommissionType {
    SystemDecommission,
    ClusterDecommission
}
