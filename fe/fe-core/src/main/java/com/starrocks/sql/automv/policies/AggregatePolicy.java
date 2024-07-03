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

package com.starrocks.sql.automv.policies;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PlanPiecePrinter;
import com.starrocks.sql.automv.util.PrettyPrinter;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface AggregatePolicy {
    AbstractAggregatePolicy IDENTITY_POLICY = new AbstractAggregatePolicy() {
        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return Optional.of(aggPiece);
        }
    };
    AbstractAggregatePolicy NONE_POLICY = new AbstractAggregatePolicy() {
        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return Optional.empty();
        }
    };

    static AndPolicy and(List<AbstractAggregatePolicy> policies) {
        return new AndPolicy(policies);
    }

    static AndPolicy and(AbstractAggregatePolicy... policies) {
        return and(ImmutableList.copyOf(policies));
    }

    static OrPolicy or(List<AbstractAggregatePolicy> policies) {
        return new OrPolicy(policies);
    }

    static OrPolicy or(AbstractAggregatePolicy... policies) {
        return or(ImmutableList.copyOf(policies));
    }

    static SeqPolicy seq(List<AbstractAggregatePolicy> policies) {
        return new SeqPolicy(policies);
    }

    static SeqPolicy seq(AbstractAggregatePolicy... policies) {
        return seq(ImmutableList.copyOf(policies));
    }

    static AggregatePolicy not(AbstractAggregatePolicy a) {
        return new NotPolicy(ImmutableList.of(a));
    }

    static AbstractAggregatePolicy trace(AbstractAggregatePolicy policy, PrettyPrinter prettyPrinter, int level) {
        Function<AbstractAggregatePolicy, AbstractAggregatePolicy> traceDecorator =
                TracePolicy.toTraceDecorator(prettyPrinter, level);
        return AssemblyPolicy.decorateImpl(policy, traceDecorator);
    }

    Optional<AggregatePiece> convert(AggregatePiece aggPiece);

    abstract class AbstractAggregatePolicy implements AggregatePolicy {
        public PrettyPrinter toPrettyString() {
            PrettyPrinter printer = new PrettyPrinter();
            printer.add(this.getClass().getSimpleName());
            return printer;
        }

        @Override
        public final String toString() {
            return toPrettyString().getResult();
        }
    }

    abstract class SimplePolicy extends AbstractAggregatePolicy {
    }

    abstract class AssemblyPolicy extends AbstractAggregatePolicy {
        private final List<AbstractAggregatePolicy> policies;

        AssemblyPolicy(final List<AbstractAggregatePolicy> policies) {
            this.policies = (policies instanceof ImmutableList) ? policies : ImmutableList.copyOf(policies);
        }

        private static AbstractAggregatePolicy decorateImpl(
                AbstractAggregatePolicy policy,
                Function<AbstractAggregatePolicy, AbstractAggregatePolicy> decorator) {
            if (policy instanceof AssemblyPolicy) {
                AssemblyPolicy assemblyPolicy = (AssemblyPolicy) policy;
                List<AbstractAggregatePolicy> newPolicies = assemblyPolicy.getPolicies().stream()
                        .map(p -> decorateImpl(p, decorator))
                        .collect(ImmutableList.toImmutableList());
                return decorator.apply(assemblyPolicy.changePolicies(newPolicies));
            } else {
                return decorator.apply(policy);
            }
        }

        public List<AbstractAggregatePolicy> getPolicies() {
            return policies;
        }

        public abstract AssemblyPolicy changePolicies(List<AbstractAggregatePolicy> newPolicies);

        @Override
        public PrettyPrinter toPrettyString() {
            PrettyPrinter printer = new PrettyPrinter();
            List<PrettyPrinter> printers = policies.stream()
                    .map(AbstractAggregatePolicy::toPrettyString).collect(Collectors.toList());
            printer.add(this.getClass().getSimpleName()).spaces(1).add("{").newLine();
            printer.indentEnclose(() -> {
                printer.addSuperStepsWithDelNl(",", printers);
            });
            printer.newLine().add("}");
            return printer;
        }
    }

    final class AndPolicy extends AssemblyPolicy {
        private AndPolicy(List<AbstractAggregatePolicy> policies) {
            super(policies);
        }

        private static AggregatePolicy andImpl(AggregatePolicy a, AggregatePolicy b) {
            return aggPiece -> a.convert(aggPiece).map(b::convert).flatMap(Function.identity());
        }

        @Override
        public AssemblyPolicy changePolicies(List<AbstractAggregatePolicy> newPolicies) {
            return new AndPolicy(newPolicies);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return getPolicies().stream().map(policy -> (AggregatePolicy) policy)
                    .reduce(IDENTITY_POLICY, AndPolicy::andImpl).convert(aggPiece);
        }
    }

    final class OrPolicy extends AssemblyPolicy {
        private OrPolicy(List<AbstractAggregatePolicy> policies) {
            super(policies);
        }

        private static AggregatePolicy orImpl(AggregatePolicy a, AggregatePolicy b) {
            return aggPiece -> {
                Optional<AggregatePiece> result = a.convert(aggPiece);
                return result.isPresent() ? result : b.convert(aggPiece);
            };
        }

        @Override
        public AssemblyPolicy changePolicies(List<AbstractAggregatePolicy> newPolicies) {
            return new OrPolicy(newPolicies);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return getPolicies().stream().map(policy -> (AggregatePolicy) policy)
                    .reduce(NONE_POLICY, OrPolicy::orImpl).convert(aggPiece);
        }
    }

    final class SeqPolicy extends AssemblyPolicy {
        private SeqPolicy(List<AbstractAggregatePolicy> policies) {
            super(policies);
        }

        private static AggregatePolicy seqImpl(AggregatePolicy a, AggregatePolicy b) {
            return aggPiece -> {
                AggregatePiece newAggPiece = a.convert(aggPiece).orElse(aggPiece);
                return Optional.of(b.convert(newAggPiece).orElse(newAggPiece));
            };
        }

        @Override
        public AssemblyPolicy changePolicies(List<AbstractAggregatePolicy> newPolicies) {
            return new SeqPolicy(newPolicies);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return getPolicies().stream().map(policy -> (AggregatePolicy) policy)
                    .reduce(IDENTITY_POLICY, SeqPolicy::seqImpl).convert(aggPiece);
        }
    }

    final class NotPolicy extends AssemblyPolicy {

        private NotPolicy(List<AbstractAggregatePolicy> policies) {
            super(policies);
            Preconditions.checkArgument(policies.size() == 1);
        }

        private static AggregatePolicy notImpl(AggregatePolicy a) {
            return aggPiece -> a.convert(aggPiece).isPresent() ?
                    NONE_POLICY.convert(aggPiece) :
                    IDENTITY_POLICY.convert(aggPiece);
        }

        @Override
        public AssemblyPolicy changePolicies(List<AbstractAggregatePolicy> newPolicies) {
            return new NotPolicy(newPolicies);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return notImpl(getPolicies().get(0)).convert(aggPiece);
        }
    }

    final class TracePolicy extends AbstractAggregatePolicy {
        private final AbstractAggregatePolicy policy;
        private final PrettyPrinter prettyPrinter;
        private final int level;

        private TracePolicy(AbstractAggregatePolicy policy, PrettyPrinter prettyPrinter, int level) {
            this.policy = Objects.requireNonNull(policy);
            this.prettyPrinter = Objects.requireNonNull(prettyPrinter);
            this.level = level;
        }

        private static AbstractAggregatePolicy trace(AbstractAggregatePolicy policy, PrettyPrinter prettyPrinter,
                                                     int level) {
            if (policy instanceof TracePolicy) {
                return policy;
            } else {
                return new TracePolicy(policy, prettyPrinter, level);
            }
        }

        static Function<AbstractAggregatePolicy, AbstractAggregatePolicy> toTraceDecorator(
                PrettyPrinter prettyPrinter, int level) {
            return policy -> TracePolicy.trace(policy, prettyPrinter, level);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            Optional<AggregatePiece> optPiece = policy.convert(aggPiece);
            if (policy.getClass().getSimpleName().isEmpty()) {
                return optPiece;
            }
            String status = optPiece.map(a -> "SUCCESS").orElse("FAIL");
            prettyPrinter.add("RUN AggregatePolicy [").add(status).add("]: ")
                    .add("'").add(policy.getClass().getSimpleName()).add("'").newLine();

            prettyPrinter.indentEnclose(() -> {
                prettyPrinter.add("PolicyDetail:").newLine();
                prettyPrinter.indentEnclose(() -> prettyPrinter.addSuperStepWithIndent(policy.toPrettyString()));
            });

            prettyPrinter.newLine();
            prettyPrinter.newLine();
            optPiece.ifPresent(piece -> {
                prettyPrinter.indentEnclose(() -> {
                    prettyPrinter.add("NewPlanPiece:");
                    prettyPrinter.indentEnclose(() -> {
                        prettyPrinter.addSuperStepWithIndent(PlanPiecePrinter.print(piece, null, level));
                    });
                });
                prettyPrinter.newLine();
            });

            return optPiece;
        }

        @Override
        public PrettyPrinter toPrettyString() {
            return policy.toPrettyString();
        }
    }
}