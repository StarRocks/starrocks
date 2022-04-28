package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.system.SystemInfoService;

public class UpdateFrontendAddressClause extends FrontendClause {

    protected String discardedHostPort;
    protected String newlyEffectiveHostPort;

    protected Pair<String, Integer> discardedHostPortPair;
    protected Pair<String, Integer> newlyEffectivePortPair;
    
    protected UpdateFrontendAddressClause(String hostPort, FrontendNodeType role) {
        super(hostPort, role);
    }

    protected UpdateFrontendAddressClause(String discardedHostPort, String newlyEffectiveHostPort) {
        super(discardedHostPort, FrontendNodeType.UNKNOWN);
        this.discardedHostPort = discardedHostPort;
        this.newlyEffectiveHostPort = newlyEffectiveHostPort;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        discardedHostPortPair = SystemInfoService.validateHostAndPort(discardedHostPort, false);
        Preconditions.checkState(!Strings.isNullOrEmpty(discardedHostPortPair.first));
        newlyEffectivePortPair = SystemInfoService.validateHostAndPort(newlyEffectiveHostPort, false);
        Preconditions.checkState(!Strings.isNullOrEmpty(newlyEffectivePortPair.first));
    }

    public Pair<String, Integer> getDiscardedHostPort() {
        return discardedHostPortPair;
    }

    public Pair<String, Integer> getNewlyEffectiveHostPort() {
        return newlyEffectivePortPair;
    }
}
