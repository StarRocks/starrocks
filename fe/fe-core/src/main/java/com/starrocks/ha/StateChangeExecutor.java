// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.ha;

import com.google.common.base.Preconditions;
import com.google.common.collect.Queues;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.Util;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class StateChangeExecutor extends Daemon {
    private static final int STATE_CHANGE_CHECK_INTERVAL_MS = 100;
    private static final Logger LOG = LogManager.getLogger(StateChangeExecutor.class);

    private BlockingQueue<FrontendNodeType> typeTransferQueue;
    private List<StateChangeExecution> executions;

    private static class SingletonHolder {
        private static final StateChangeExecutor INSTANCE = new StateChangeExecutor();
    }

    public static StateChangeExecutor getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public StateChangeExecutor() {
        super("stateChangeExecutor", STATE_CHANGE_CHECK_INTERVAL_MS);
        typeTransferQueue = Queues.newLinkedBlockingDeque();
        executions = new ArrayList<>();
    }

    public void registerStateChangeExecution(StateChangeExecution execution) {
        executions.add(execution);
    }

    public void notifyNewFETypeTransfer(FrontendNodeType newType) {
        try {
            String msg = "notify new FE type transfer: " + newType;
            LOG.warn(msg);
            Util.stdoutWithTime(msg);
            typeTransferQueue.put(newType);
        } catch (InterruptedException e) {
            LOG.error("failed to put new FE type: {}, {}.", newType, e);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected void runOneCycle() {
        while (true) {
            FrontendNodeType newType = null;
            try {
                newType = typeTransferQueue.take();
            } catch (InterruptedException e) {
                LOG.error("got exception when take FE type from queue", e);
                Thread.currentThread().interrupt();
                Util.stdoutWithTime("got exception when take FE type from queue. " + e.getMessage());
                System.exit(-1);
            }
            Preconditions.checkNotNull(newType);
            FrontendNodeType feType = GlobalStateMgr.getCurrentState().getFeType();
            LOG.info("begin to transfer FE type from {} to {}", feType, newType);
            if (feType == newType) {
                return;
            }

            /*
             * INIT -> LEADER: transferToLeader
             * INIT -> FOLLOWER/OBSERVER: transferToNonLeader
             * UNKNOWN -> LEADER: transferToLeader
             * UNKNOWN -> FOLLOWER/OBSERVER: transferToNonLeader
             * FOLLOWER -> LEADER: transferToLeader
             * FOLLOWER/OBSERVER -> INIT/UNKNOWN: set isReady to false
             */
            switch (feType) {
                case INIT: {
                    switch (newType) {
                        case LEADER: {
                            for (StateChangeExecution execution : executions) {
                                execution.transferToLeader();
                            }
                            break;
                        }
                        case FOLLOWER:
                        case OBSERVER: {
                            for (StateChangeExecution execution : executions) {
                                execution.transferToNonLeader(newType);
                            }
                            break;
                        }
                        case UNKNOWN:
                            break;
                        default:
                            break;
                    }
                    break;
                }
                case UNKNOWN: {
                    switch (newType) {
                        case LEADER: {
                            for (StateChangeExecution execution : executions) {
                                execution.transferToLeader();
                            }
                            break;
                        }
                        case FOLLOWER:
                        case OBSERVER: {
                            for (StateChangeExecution execution : executions) {
                                execution.transferToNonLeader(newType);
                            }
                            break;
                        }
                        default:
                            break;
                    }
                    break;
                }
                case FOLLOWER: {
                    switch (newType) {
                        case LEADER: {
                            for (StateChangeExecution execution : executions) {
                                execution.transferToLeader();
                            }
                            break;
                        }
                        case UNKNOWN: {
                            for (StateChangeExecution execution : executions) {
                                execution.transferToNonLeader(newType);
                            }
                            break;
                        }
                        default:
                            break;
                    }
                    break;
                }
                case OBSERVER: {
                    if (newType == FrontendNodeType.UNKNOWN) {
                        for (StateChangeExecution execution : executions) {
                            execution.transferToNonLeader(newType);
                        }
                    }
                    break;
                }
                case LEADER: {
                    // exit if leader changed to any other type
                    String msg = "transfer FE type from LEADER to " + newType.name() + ". exit";
                    LOG.error(msg);
                    Util.stdoutWithTime(msg);
                    System.exit(-1);
                }
                default:
                    break;
            } // end switch formerFeType

            LOG.info("finished to transfer FE type from {} to {}", feType, newType);
        }
    } // end runOneCycle
}
