package com.tradingplatform.pipeline.store;

import com.tradingplatform.pipeline.events.ExecutionCreated;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The record of trades the pipeline has seen.
 *
 * <p>Idempotent by construction: the execution id is the primary key, so a redelivery is a
 * {@code putIfAbsent} that does nothing. There is no separate "have I seen this?" bookkeeping to
 * get out of step with the data, which is the failure mode a side-table of processed ids invites.
 *
 * <p>In-memory for Stage 4. Stage 5 replaces it with a table whose primary key enforces the same
 * property in the database rather than in a map.
 */
public final class ExecutionStore {
    private final Map<Long, ExecutionCreated> executionsById = new ConcurrentHashMap<>();

    /** @return true if this call recorded the execution, false if it was already known */
    public boolean record(ExecutionCreated execution) {
        return executionsById.putIfAbsent(execution.executionId(), execution) == null;
    }

    public Optional<ExecutionCreated> find(long executionId) {
        return Optional.ofNullable(executionsById.get(executionId));
    }

    public Collection<ExecutionCreated> all() {
        return Map.copyOf(executionsById).values();
    }

    public int size() {
        return executionsById.size();
    }
}
