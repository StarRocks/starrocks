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

package com.starrocks.sql.automv.generator;

import com.starrocks.sql.automv.util.Util;

import java.util.Objects;
import java.util.Optional;

public class MVName {
    private static final String MV_NAME_PREFIX = "_mv_";
    private static final int NAME_LENGTH = 60;
    private final String prefix;
    private final String createTime;
    private final String digest;

    private MVName(String prefix, String ctime, String digest) {
        this.prefix = Objects.requireNonNull(prefix);
        this.createTime = Objects.requireNonNull(ctime);
        this.digest = Objects.requireNonNull(digest);
    }

    public static MVName generateFromQuery(String query) {
        String digest = Util.sha1(query);
        return new MVName(MV_NAME_PREFIX, Util.yyyyMMddTHHmmss(), digest);
    }

    public static Optional<MVName> parse(String s) {
        if (s == null || s.length() != NAME_LENGTH || !s.startsWith(MV_NAME_PREFIX)) {
            return Optional.empty();
        }
        String createTime = s.substring(MV_NAME_PREFIX.length(), MV_NAME_PREFIX.length() + 15);
        String digest = s.substring(MV_NAME_PREFIX.length() + 16);
        boolean legalCreateTime = Util.yyyyMMddTHHmmssToDate(createTime)
                .map(dt -> Util.yyyyMMddTHHmmss(dt).equals(createTime))
                .orElse(false);
        boolean legalDigest = Util.isHexString(digest);
        if (legalCreateTime && legalDigest) {
            return Optional.of(new MVName(MV_NAME_PREFIX, createTime, digest));
        } else {
            return Optional.empty();
        }
    }

    public String getPrefix() {
        return prefix;
    }

    public String getDigest() {
        return digest;
    }

    public String getCreateTime() {
        return createTime;
    }

    public boolean collidesWith(MVName that) {
        return that.digest.equals(this.digest);
    }

    public boolean collidesWith(String mvName) {
        return parse(mvName).map(this::collidesWith).orElse(false);
    }

    @Override
    public String toString() {
        return prefix + createTime + "_" + digest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MVName mvName = (MVName) o;
        return Objects.equals(prefix, mvName.prefix) && Objects.equals(createTime, mvName.createTime) &&
                Objects.equals(digest, mvName.digest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(prefix, createTime, digest);
    }
}
