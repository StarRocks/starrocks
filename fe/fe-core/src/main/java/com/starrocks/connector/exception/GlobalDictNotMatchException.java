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

package com.starrocks.connector.exception;

import com.starrocks.common.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

public class GlobalDictNotMatchException extends RuntimeException {
    private static final Logger LOG = LogManager.getLogger(GlobalDictNotMatchException.class);
    public GlobalDictNotMatchException(String message) {
        super(message);
    }

    public String getErrorMessage() {
        return super.getMessage();
    }

    @Override
    public String getMessage() {
        return getErrorMessage();
    }

    public Pair<Optional<Integer>, Optional<String>> extract() {
        try {
            String[] splits = getMessage().split(",");
            Optional<Integer> slotId = Optional.empty();
            Optional<String> fileName = Optional.empty();
            for (String split : splits) {
                if (split.strip().startsWith("SlotId: ")) {
                    slotId = Optional.of(Integer.parseInt(split.replace("SlotId: ", "").strip()));
                } else if (split.strip().startsWith("FileName: ")) {
                    fileName = Optional.of(split.replace("FileName: ", "").strip());
                }
            }
            return new Pair<>(slotId, fileName);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return new Pair<Optional<Integer>, Optional<String>>(Optional.empty(), Optional.empty());
        }
    }
}