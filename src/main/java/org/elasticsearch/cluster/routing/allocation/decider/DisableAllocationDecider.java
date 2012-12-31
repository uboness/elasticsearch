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
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

/**
 * This {@link AllocationDecider} prevents cluster-wide shard allocations. The
 * behavior of this {@link AllocationDecider} can be changed in real-time via
 * the cluster settings API. It respects the following settings:
 * <ul>
 * <li><tt>cluster.routing.allocation.disable_new_allocation</tt> - if set to
 * <code>true</code> no new shard-allocation are allowed. Note: this setting is
 * only applied if the allocated shard is a primary and it has not been
 * allocated before the this setting was applied.</li>
 * 
 * <li><tt>cluster.routing.allocation.disable_allocation</tt> - if set to
 * <code>true</code> cluster wide allocations are disabled</li>
 * 
 * <li><tt>cluster.routing.allocation.disable_replica_allocation</tt> - if set
 * to <code>true</code> cluster wide replica allocations are disabled while
 * primary shards can still be allocated</li>
 * </ul>
 * 
 * <p>
 * Note: all of the above settings might be ignored if the allocation happens on
 * a shard that explicitly ignores disabled allocations via
 * {@link RoutingAllocation#ignoreDisable()}. Which is set if allocation are
 * explicit.
 * </p>
 */
public class DisableAllocationDecider extends AllocationDecider {

    private final static String NAME = "disable allocation";

    private final static String DISABLE_NEW_KEY = "cluster.routing.allocation.disable_new_allocation";
    private final static String DISABLE_KEY = "cluster.routing.allocation.disable_allocation";
    private final static String DISABLE_REPLICA_KEY = "cluster.routing.allocation.disable_replica_allocation";

    static {
        MetaData.addDynamicSettings(
                DISABLE_NEW_KEY,
                DISABLE_KEY,
                DISABLE_REPLICA_KEY
        );
    }

    private final static Decision YES = Decision.yes(NAME, "allocation is not disabled");
    private final static Decision DISABLE_NEW_DECISION = Decision.no(NAME, "allocation of newly created shards is disabled ([%s] is set to [true])", DISABLE_NEW_KEY);
    private final static Decision DISABLE_DECISION = Decision.no(NAME, "allocation is disabled ([%s] is set to [true]", DISABLE_KEY);
    private final static Decision DISABLE_REPLICA_DECISION = Decision.no(NAME, "allocation of replicas is disabled ([%s] is set to [true]", DISABLE_REPLICA_KEY);

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            boolean disableNewAllocation = settings.getAsBoolean(DISABLE_NEW_KEY, DisableAllocationDecider.this.disableNewAllocation);
            if (disableNewAllocation != DisableAllocationDecider.this.disableNewAllocation) {
                logger.info("updating [{}] from [{}] to [{}]", DISABLE_NEW_KEY, DisableAllocationDecider.this.disableNewAllocation, disableNewAllocation);
                DisableAllocationDecider.this.disableNewAllocation = disableNewAllocation;
            }

            boolean disableAllocation = settings.getAsBoolean(DISABLE_KEY, DisableAllocationDecider.this.disableAllocation);
            if (disableAllocation != DisableAllocationDecider.this.disableAllocation) {
                logger.info("updating [{}] from [{}] to [{}]", DISABLE_KEY, DisableAllocationDecider.this.disableAllocation, disableAllocation);
                DisableAllocationDecider.this.disableAllocation = disableAllocation;
            }

            boolean disableReplicaAllocation = settings.getAsBoolean(DISABLE_REPLICA_KEY, DisableAllocationDecider.this.disableReplicaAllocation);
            if (disableReplicaAllocation != DisableAllocationDecider.this.disableReplicaAllocation) {
                logger.info("updating [{}] from [{}] to [{}]", DISABLE_REPLICA_KEY, DisableAllocationDecider.this.disableReplicaAllocation, disableReplicaAllocation);
                DisableAllocationDecider.this.disableReplicaAllocation = disableReplicaAllocation;
            }
        }
    }

    private volatile boolean disableNewAllocation;
    private volatile boolean disableAllocation;
    private volatile boolean disableReplicaAllocation;

    @Inject
    public DisableAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(NAME, settings);
        this.disableNewAllocation = settings.getAsBoolean("cluster.routing.allocation.disable_new_allocation", false);
        this.disableAllocation = settings.getAsBoolean("cluster.routing.allocation.disable_allocation", false);
        this.disableReplicaAllocation = settings.getAsBoolean("cluster.routing.allocation.disable_replica_allocation", false);

        nodeSettingsService.addListener(new ApplySettings());
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.ignoreDisable()) {
            return Decision.NOT_APPLICABLE;
        }
        if (shardRouting.primary() && !allocation.routingNodes().routingTable().index(shardRouting.index()).shard(shardRouting.id()).primaryAllocatedPostApi()) {
            // if its primary, and it hasn't been allocated post API (meaning its a "fresh newly created shard"), only disable allocation
            // on a special disable allocation flag
            if (disableNewAllocation) {
                return DISABLE_NEW_DECISION;
            }
            return YES;
        }
        if (disableAllocation) {
            return DISABLE_DECISION;
        }
        if (disableReplicaAllocation) {
            return DISABLE_REPLICA_DECISION;
        }
        return YES;
    }
}
