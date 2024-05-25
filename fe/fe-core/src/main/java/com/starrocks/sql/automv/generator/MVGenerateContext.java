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

import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.util.PrettyPrinter;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class MVGenerateContext {
    private final AutoMVOptions options;
    private final PrettyPrinter generateTraceLog;
    private final PrettyPrinter policyTraceLog;
    private final Function<String, String> mvNameGenerator;
    private final Supplier<Integer> nextId;

    private MVGenerateContext(Builder builder) {
        this.options = Objects.requireNonNull(builder.options);
        this.policyTraceLog = builder.policyTraceLog;
        this.generateTraceLog = builder.generateTraceLog;
        this.mvNameGenerator = Objects.requireNonNull(builder.mvNameGenerator);
        this.nextId = Objects.requireNonNull(builder.nextId);
    }

    public static Builder builder() {
        return new Builder();
    }

    public AutoMVOptions getOptions() {
        return options;
    }

    public boolean isEnableGenerateTraceLog() {
        return generateTraceLog != null;
    }

    public boolean isEnablePolicyTraceLog() {
        return policyTraceLog != null;
    }

    public Optional<PrettyPrinter> getGenerateTraceLog() {
        return Optional.ofNullable(generateTraceLog);
    }

    public Optional<PrettyPrinter> getPolicyTraceLog() {
        return Optional.ofNullable(policyTraceLog);
    }

    public Function<String, String> getMvNameGenerator() {
        return mvNameGenerator;
    }

    public Supplier<Integer> getNextId() {
        return Objects.requireNonNull(nextId);
    }

    public static class Builder {
        private AutoMVOptions options;
        private PrettyPrinter generateTraceLog;
        private PrettyPrinter policyTraceLog;
        private Function<String, String> mvNameGenerator;
        private Supplier<Integer> nextId;

        private Builder() {
        }

        public Builder setOptions(AutoMVOptions options) {
            this.options = Objects.requireNonNull(options);
            return this;
        }

        public Builder enableGenerateTraceLog() {
            this.generateTraceLog = new PrettyPrinter();
            return this;
        }

        public Builder enablePolicyTraceLog() {
            this.policyTraceLog = new PrettyPrinter();
            return this;
        }

        public Builder setMvNameGenerator(Function<String, String> mvNameGenerator) {
            this.mvNameGenerator = Objects.requireNonNull(mvNameGenerator);
            return this;
        }

        public Builder setNextId(Supplier<Integer> nextId) {
            this.nextId = Objects.requireNonNull(nextId);
            return this;
        }

        public MVGenerateContext build() {
            return new MVGenerateContext(this);
        }
    }
}
