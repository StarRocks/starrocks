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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.GetRemoteFilesParams;

public class IcebergGetRemoteFilesParams extends GetRemoteFilesParams {
    private IcebergTableMORParams tableFullMORParams;
    private IcebergMORParams morParams;

    private IcebergGetRemoteFilesParams(Builder builder) {
        super(builder);
        this.tableFullMORParams = builder.tableFullMORParams;
        this.morParams = builder.morParams;
    }

    public IcebergTableMORParams getTableFullMORParams() {
        return tableFullMORParams;
    }

    public IcebergMORParams getMORParams() {
        return morParams;
    }

    public static class Builder extends GetRemoteFilesParams.Builder {
        private IcebergTableMORParams tableFullMORParams;
        private IcebergMORParams morParams;

        public IcebergGetRemoteFilesParams.Builder setParams(IcebergMORParams morParams) {
            this.morParams = morParams;
            return this;
        }

        public IcebergGetRemoteFilesParams.Builder setAllParams(IcebergTableMORParams tableFullMORParams) {
            this.tableFullMORParams = tableFullMORParams;
            return this;
        }

        public GetRemoteFilesParams build() {
            return new IcebergGetRemoteFilesParams(this);
        }
    }

    public static IcebergGetRemoteFilesParams.Builder newBuilder() {
        return new IcebergGetRemoteFilesParams.Builder();
    }
}
