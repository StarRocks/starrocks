// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.transaction;

<<<<<<< HEAD
import com.starrocks.common.UserException;
=======
import com.starrocks.common.StarRocksException;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

public abstract class AbstractTxnStateChangeCallback implements TxnStateChangeCallback {
    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {

    }

    @Override
    public void beforeAborted(TransactionState txnState) throws TransactionException {

    }

    @Override
<<<<<<< HEAD
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws UserException {
=======
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws StarRocksException {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {

    }

    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
<<<<<<< HEAD
            throws UserException {
=======
            throws StarRocksException {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    }

    @Override
    public void replayOnAborted(TransactionState txnState) {

    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {

    }

    @Override
    public void replayOnVisible(TransactionState txnState) {

    }

    @Override
    public void beforePrepared(TransactionState txnState) throws TransactionException {

    }

    @Override
<<<<<<< HEAD
    public void afterPrepared(TransactionState txnState, boolean txnOperated) throws UserException {
=======
    public void afterPrepared(TransactionState txnState, boolean txnOperated) throws StarRocksException {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    }

    @Override
    public void replayOnPrepared(TransactionState txnState) {

    }
}
