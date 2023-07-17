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

package com.starrocks.qe.scheduler.state.statemachine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.qe.scheduler.state.JobState;
import com.starrocks.qe.scheduler.state.event.JobEvent;
import com.starrocks.qe.scheduler.state.event.JobEventType;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class StateMachineFactory {

    private final Map<JobEventType, Map<Class<? extends JobState>, Transition>> transitionMap;

    public StateMachineFactory(JobState initialState) {
        this.transitionMap = Maps.newHashMap();
    }

    public StateMachineFactory addTransition(Set<Class<? extends JobState>> fromStates,
                                             JobEventType event, Class<? extends JobState> toState,
                                             TransitionAction action) {
        fromStates.forEach(fromState -> addTransition(fromState, event, toState, action));
        return this;
    }

    public StateMachineFactory addTransition(Class<? extends JobState> fromState,
                                             JobEventType event, Class<? extends JobState> toState,
                                             TransitionAction action) {
        return addTransition(fromState, event, ImmutableSet.of(toState), action);
    }

    public StateMachineFactory addTransition(Class<? extends JobState> fromState,
                                             JobEventType event, Set<Class<? extends JobState>> toStates,
                                             TransitionAction action) {
        transitionMap
                .computeIfAbsent(event, k -> Maps.newHashMap())
                .put(fromState, new Transition(action, toStates));
        return this;
    }

    public JobStateMachine create(StateContext ctx, JobState initialState, List<StateTransitionListener> listeners) {
        return new InternalStateMachine(ctx, initialState, transitionMap, listeners);
    }

    public JobStateMachine create(StateContext ctx, JobState initialState) {
        return create(ctx, initialState, ImmutableList.of());
    }

    private static class Transition {
        private final TransitionAction action;
        private final Set<Class<? extends JobState>> candidateToStates;

        public Transition(TransitionAction action, Set<Class<? extends JobState>> candidateToStates) {
            this.action = action;
            this.candidateToStates = candidateToStates;
        }

        JobState apply(StateContext ctx, JobState fromState, JobEvent event) throws InvalidJobStateTransitionException {
            JobState toState = action.transition(ctx, fromState, event);
            if (!candidateToStates.contains(toState.getClass())) {
                throw new InvalidJobStateTransitionException(fromState.getType(), event.getType());
            }
            return toState;
        }
    }

    private static class InternalStateMachine implements JobStateMachine {
        private final StateContext ctx;
        private final Map<JobEventType, Map<Class<? extends JobState>, Transition>> transitionMap;
        private final List<StateTransitionListener> listeners;
        private JobState state;

        private InternalStateMachine(StateContext ctx, JobState initialState,
                                     Map<JobEventType, Map<Class<? extends JobState>, Transition>> transitionMap,
                                     List<StateTransitionListener> listeners) {
            this.ctx = ctx;
            this.state = initialState;
            this.transitionMap = transitionMap;
            this.listeners = listeners;
        }

        @Override
        public JobState getState() {
            return state;
        }

        @Override
        public JobState transition(JobEvent event) throws InvalidJobStateTransitionException {
            Map<Class<? extends JobState>, Transition> eventTransitionMap = transitionMap.get(event.getType());
            if (eventTransitionMap != null) {
                Transition transition = eventTransitionMap.get(state.getClass());
                if (transition != null) {
                    return doTransition(event, transition);
                }
            }

            throw new InvalidJobStateTransitionException(state.getType(), event.getType());
        }

        private JobState doTransition(JobEvent event, Transition transition) throws InvalidJobStateTransitionException {
            JobState fromState = state;

            listeners.forEach(listener -> listener.preTransition(fromState, event));
            JobState toState = transition.apply(ctx, state, event);
            listeners.forEach(listener -> listener.postTransition(fromState, event, toState));

            state = toState;

            return state;
        }
    }

}
