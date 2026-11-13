package com.bank.txn.analytics.health;

import java.util.List;

/**
 * Kafka cluster health for the dashboard.
 *
 * @param brokers               every known broker (from topic replica lists), with up/down + partitions it leads
 * @param activeControllerId    KRaft quorum leader (active controller); -1 if unknown
 * @param controllerVoters      KRaft voters (controller quorum members)
 * @param underReplicated       partitions whose ISR is smaller than the replica set
 * @param offlinePartitions     partitions without a leader
 * @param consumerLag           records not yet processed by the analytics stream (transactions.raw)
 */
public record ClusterHealth(long timestamp, boolean reachable, List<BrokerStatus> brokers, int activeControllerId,
                            List<Integer> controllerVoters, int totalPartitions, int underReplicated,
                            int offlinePartitions, long consumerLag, String error) {

    public record BrokerStatus(int id, String host, boolean up, int leaderPartitions, int replicaPartitions) {
    }

    public static ClusterHealth unreachable(String error) {
        return new ClusterHealth(System.currentTimeMillis(), false, List.of(), -1, List.of(), 0, 0, 0, -1, error);
    }
}
