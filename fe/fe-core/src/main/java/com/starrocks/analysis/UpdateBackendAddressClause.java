// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import java.util.ArrayList;
import java.util.List;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.system.SystemInfoService;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class UpdateBackendAddressClause extends BackendClause {

    protected String discardedHostPort;
    protected String newlyEffectiveHostPort;

    protected Pair<String, Integer> discardedHostPortPair;
    protected Pair<String, Integer> newlyEffectivePortPair;
    
    
    public UpdateBackendAddressClause(Pair<String, Integer> dPair, Pair<String, Integer> nPair) {
        super(new ArrayList<String>());
        discardedHostPortPair = dPair;
        newlyEffectivePortPair = nPair;
    }

    protected UpdateBackendAddressClause(List<String> hostPorts) {
        super(new ArrayList<String>());
    }

    protected UpdateBackendAddressClause(String discardedHostPort, String newlyEffectiveHostPort) {
        super(new ArrayList<String>());
        this.discardedHostPort = discardedHostPort;
        this.newlyEffectiveHostPort = newlyEffectiveHostPort;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        discardedHostPortPair = SystemInfoService.validateHostAndPort(discardedHostPort);
        Preconditions.checkState(!Strings.isNullOrEmpty(discardedHostPortPair.first));
        newlyEffectivePortPair = SystemInfoService.validateHostAndPort(newlyEffectiveHostPort);
        Preconditions.checkState(!Strings.isNullOrEmpty(newlyEffectivePortPair.first));
    }

    public Pair<String, Integer> getDiscardedHostPort() {
        return discardedHostPortPair;
    }

    public Pair<String, Integer> getNewlyEffectiveHostPort() {
        return newlyEffectivePortPair;
    }
}
