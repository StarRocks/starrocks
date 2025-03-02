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

package com.starrocks.authentication;

import com.nimbusds.jose.jwk.JWKSet;
import com.starrocks.StarRocksFE;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.ParseException;

public class JwkMgr {
    public JWKSet getJwkSet(String jwksUrl) throws IOException, ParseException {
        InputStream jwksInputStream = null;
        try {
            if (jwksUrl.startsWith("http://") || jwksUrl.startsWith("https://")) {
                jwksInputStream = new URL(jwksUrl).openStream();
            } else {
                String filePath = StarRocksFE.STARROCKS_HOME_DIR + "/conf/" + jwksUrl;
                jwksInputStream = new FileInputStream(filePath);
            }
            return JWKSet.load(jwksInputStream);
        } finally {
            if (jwksInputStream != null) {
                jwksInputStream.close();
            }
        }
    }
}
