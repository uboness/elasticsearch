/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.List;

/**
 * {@link ThrottlingAllocationDecider} controls the recovery process per node in
 * the cluster. It exposes two settings via the cluster update API that allow
 * changes in real-time:
 * 
 * <ul>
 * <li><tt>cluster.routing.allocation.node_initial_primaries_recoveries</tt> -
 * restricts the number of initial primary shard recovery operations on a single
 * node. The default is <tt>4</tt></li>
 * 
 * <li><tt>cluster.routing.allocation.node_concurrent_recoveries</tt> -
 * restricts the number of concurrent recovery operations on a single node. The
 * default is <tt>2</tt></li>
 * </ul>
 * 
 * If one of the above thresholds is exceeded per node this allocation decider
 * will return a decision of type {@link Decision.Type#THROTTLE} as a hint to upstream logic to throttle
 * the allocation process to prevent overloading nodes due to too many concurrent recovery
 * processes.
 */
public class ThrottlingAllocationDecider extends AllocationDecider {

    private final static String NAME = "throttling";

    static {
        MetaData.addDynamicSettings(
                "cluster.routing.allocation.node_initial_primaries_recoveries",
                "cluster.routing.allocation.node_concurrent_recoveries"
        );
    }

    private final static Decision YES = Decision.yes(NAME, "");
    private final static Decision THROTTLE = Decision.throttle(NAME, "");

    private volatile int primariesInitialRecoveries;
    private volatile int concurrentRecoveries;

    @Inject
    public ThrottlingAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(NAME, settings);

        this.primariesInitialRecoveries = settings.getAsInt("cluster.routing.allocation.node_initial_primaries_recoveries", settings.getAsInt("cluster.routing.allocation.node_initial_primaries_recoveries", 4));
        this.concurrentRecoveries = settings.getAsInt("cluster.routing.allocation.concurrent_recoveries", settings.getAsInt("cluster.routing.allocation.node_concurrent_recoveries", 2));
        logger.debug("using node_concurrent_recoveries [{}], node_initial_primaries_recoveries [{}]", concurrentRecoveries, primariesInitialRecoveries);

        nodeSettingsService.addListener(new ApplySettings());
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            boolean primaryUnassigned = false;
            for (MutableShardRouting shard : allocation.routingNodes().unassigned()) {
                if (shard.shardId().equals(shardRouting.shardId())) {
                    primaryUnassigned = true;
                }
            }
            if (primaryUnassigned) {
                // primary is unassigned, means we are going to do recovery from gateway
                // count *just the primary* currently doing recovery on the node and check against concurrent_recoveries
                int primariesInRecovery = 0;
                List<MutableShardRouting> shards = node.shards();
                for (int i = 0; i < shards.size(); i++) {
                    MutableShardRouting shard = shards.get(i);
                    if (shard.state() == ShardRoutingState.INITIALIZING && shard.primary()) {
                        primariesInRecovery++;
                    }
                }
                if (primariesInRecovery >= primariesInitialRecoveries) {
                    return allocation.decisionDebug(THROTTLE, "the currently ongoing primary shard recoveries count has reached the currently set limit [%s]", primariesInitialRecoveries);
                }

                return allocation.decisionTrace(YES, "the currently ongoing primary shard recoveries count is [%s] as has not yet reached the currently set limit [%s]", primariesInRecovery, primariesInitialRecoveries);
            }
        }

        // either primary or replica doing recovery (from peer shard)

        // count the number of recoveries on the node, its for both target (INITIALIZING) and source (RELOCATING)
        int currentRecoveries = 0;
        List<MutableShardRouting> shards = node.shards();
        for (int i = 0; i < shards.size(); i++) {
            MutableShardRouting shard = shards.get(i);
            if (shard.state() == ShardRoutingState.INITIALIZING || shard.state() == ShardRoutingState.RELOCATING) {
                currentRecoveries++;
            }
        }

        if (currentRecoveries >= concurrentRecoveries) {
            return allocation.decisionDebug(THROTTLE, "the currently ongoing (concurrent) shard recoveries count has reached to currently set limit [%s]", concurrentRecoveries);
        }

        return allocation.decisionTrace(YES, "the currently ongoing (concurrent) shard recoveries count [%s] has not yet reached the set limit [%s]", currentRecoveries, concurrentRecoveries);
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            int primariesInitialRecoveries = settings.getAsInt("cluster.routing.allocation.node_initial_primaries_recoveries", ThrottlingAllocationDecider.this.primariesInitialRecoveries);
            if (primariesInitialRecoveries != ThrottlingAllocationDecider.this.primariesInitialRecoveries) {
                logger.info("updating [cluster.routing.allocation.node_initial_primaries_recoveries] from [{}] to [{}]", ThrottlingAllocationDecider.this.primariesInitialRecoveries, primariesInitialRecoveries);
                ThrottlingAllocationDecider.this.primariesInitialRecoveries = primariesInitialRecoveries;
            }

            int concurrentRecoveries = settings.getAsInt("cluster.routing.allocation.node_concurrent_recoveries", ThrottlingAllocationDecider.this.concurrentRecoveries);
            if (concurrentRecoveries != ThrottlingAllocationDecider.this.concurrentRecoveries) {
                logger.info("updating [cluster.routing.allocation.node_concurrent_recoveries] from [{}] to [{}]", ThrottlingAllocationDecider.this.concurrentRecoveries, concurrentRecoveries);
                ThrottlingAllocationDecider.this.concurrentRecoveries = concurrentRecoveries;
            }
        }
    }
}
