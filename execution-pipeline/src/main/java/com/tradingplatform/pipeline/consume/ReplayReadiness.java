package com.tradingplatform.pipeline.consume;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;

/**
 * Tracks which consumer groups have finished their startup replay.
 *
 * <p>This matters because the risk engine's positions are rebuilt by replaying the executions topic
 * from the beginning. A gateway that accepted orders during that replay would be checking limits
 * against a position still being reconstructed, and would let an account trade past a limit it had
 * already reached before the restart.
 *
 * <p>"Caught up" is inferred from the container going idle: the consumer has been assigned
 * partitions and has stopped receiving records. That is a signal rather than a proof — a genuinely
 * quiet topic looks the same as a finished replay — but it needs no admin client and no end-offset
 * polling, and it errs in the safe direction: a backlog keeps the gateway un-ready rather than
 * declaring readiness early.
 */
public final class ReplayReadiness {
    private static final Logger LOG = LoggerFactory.getLogger(ReplayReadiness.class);

    private static final Pattern CHILD_SUFFIX = Pattern.compile("-\\d+$");

    private final Set<String> caughtUpGroups = ConcurrentHashMap.newKeySet();

    @EventListener
    public void onContainerIdle(ListenerContainerIdleEvent event) {
        String group = groupOf(event.getListenerId());
        if (group != null && caughtUpGroups.add(group)) {
            LOG.info("event=replay_caught_up consumer={}", group);
        }
    }

    /**
     * A concurrent container names its children {@code <group>-0}, {@code <group>-1} and so on, and
     * it is the child that reports idleness. Callers ask about the group, so the suffix is stripped
     * here rather than at every call site.
     *
     * <p>One idle child is treated as the group being caught up. With partitions spread across
     * children that is optimistic, but the alternative — waiting for every child — never completes
     * when there are more children than partitions.
     */
    private static String groupOf(String listenerId) {
        return listenerId == null ? null : CHILD_SUFFIX.matcher(listenerId).replaceFirst("");
    }

    public boolean isCaughtUp(String group) {
        return caughtUpGroups.contains(group);
    }

    public Set<String> caughtUpGroups() {
        return Set.copyOf(caughtUpGroups);
    }
}
