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

package com.starrocks.connector.paimon;

import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.paimon.index.PaimonGlobalIndexResult;

public final class PaimonGetRemoteFilesParams extends GetRemoteFilesParams {
    private final boolean disableGlobalIndex;
    private PaimonGlobalIndexResult indexResult;

    private PaimonGetRemoteFilesParams(Builder builder) {
        super(builder);
        this.disableGlobalIndex = builder.disableGlobalIndex;
        this.indexResult = builder.indexResult;
    }

    public boolean isDisableGlobalIndex() {
        return disableGlobalIndex;
    }

    public PaimonGlobalIndexResult getIndexResult() {
        return indexResult;
    }

    public void setIndexResult(PaimonGlobalIndexResult indexResult) {
        this.indexResult = indexResult;
    }

    @Override
    public PaimonGetRemoteFilesParams copy() {
        Builder builder = newBuilder().setDisableGlobalIndex(disableGlobalIndex).setIndexResult(indexResult);
        copyTo(builder);
        return builder.build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder extends GetRemoteFilesParams.Builder {
        private boolean disableGlobalIndex;
        private PaimonGlobalIndexResult indexResult;

        public Builder setDisableGlobalIndex(boolean disableGlobalIndex) {
            this.disableGlobalIndex = disableGlobalIndex;
            return this;
        }

        public Builder setIndexResult(PaimonGlobalIndexResult indexResult) {
            this.indexResult = indexResult;
            return this;
        }

        @Override
        public PaimonGetRemoteFilesParams build() {
            return new PaimonGetRemoteFilesParams(this);
        }
    }
}
