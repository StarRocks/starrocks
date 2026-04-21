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
package com.starrocks.epack.system;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class LicenseMgrPersist {

    @SerializedName("licenses")
    List<String> licenses;

    @SerializedName("isEncrypted")
    boolean isEncrypted;

    @SerializedName("systemInfoStr")
    String systemInfoStr;

    @SerializedName("scale_out_license_free_start_time")
    Long scaleOutLicenseFreeStartTime;

    public LicenseMgrPersist(List<String> licenses, boolean isEncrypted, String systemInfoStr,
                             Long scaleOutLicenseFreeStartTime) {
        this.licenses = licenses;
        this.isEncrypted = isEncrypted;
        this.systemInfoStr = systemInfoStr;
        this.scaleOutLicenseFreeStartTime = scaleOutLicenseFreeStartTime;
    }
}
