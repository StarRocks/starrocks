// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.transformer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CTETransformerContext {
    private final Map<Integer, ExpressionMapping> cteExpressions;

    private final Map<Integer, Integer> cteRefId;
    private final AtomicInteger uniqueId;

    public CTETransformerContext() {
        this.cteExpressions = new HashMap<>();
        this.cteRefId = new HashMap<>();
        this.uniqueId = new AtomicInteger();
    }

    public CTETransformerContext(CTETransformerContext other) {
        // This must be a copy of the context, because the new Relation may contain cte with the same name,
        // and the internal cte with the same name will overwrite the original mapping
        this.cteExpressions = Maps.newHashMap(other.cteExpressions);
        // must use one instance
        this.cteRefId = other.cteRefId;
        this.uniqueId = other.uniqueId;
    }

    public Map<Integer, ExpressionMapping> getCteExpressions() {
        return cteExpressions;
    }

    /*
     * Regenerate cteID, we do inline CTE in transform phase, needs
     * to be promised that the cteId generated each time is different
     *
     * e.g.
     *   with x1 (
     *       with x2 (select * from t0) select * from x2
     *   )
     *   select * from x1;
     *
     * Transform result without inline cte:
     *
     *                             CTEAnchor1
     *                          /             \
     *                    CTEProduce1         CTEConsume1
     *                       /
     *                CTEAnchor2
     *                 /       \
     *         CTEProduce2    CTEConsume2
     *               /
     *          Scan(t0)
     *
     * But we do inline cte in transform phase now, the result will be:
     *                             CTEAnchor1
     *                          /             \
     *                    CTEProduce1         CTEConsume1
     *                       /                        \
     *                CTEAnchor2                    CTEAnchor2-1
     *                 /       \                     /       \
     *         CTEProduce2    CTEConsume2    CTEProduce2-1    CTEConsume2-1
     *               /             \               /             \
     *          Scan(t0)        Scan(t0)      Scan(t0)           Scan(t0)
     *
     *  CTEAnchor2 and CTEAnchor2-1 is from same CTE (with x2), but has different cteID
     *  So, modify the cteID everytime on one CTE instance.
     */
    public void registerCteRef(int cteId) {
        cteRefId.put(cteId, uniqueId.incrementAndGet());
    }

    public int getCteRefId(int cteId) {
        Preconditions.checkState(cteRefId.containsKey(cteId));
        return cteRefId.get(cteId);
    }
}
