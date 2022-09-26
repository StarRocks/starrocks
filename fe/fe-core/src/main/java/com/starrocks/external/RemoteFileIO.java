// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

import java.util.List;
import java.util.Map;

public interface RemoteFileIO {

    Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey);
}
