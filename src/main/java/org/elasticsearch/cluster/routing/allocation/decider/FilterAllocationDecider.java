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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.OR;

/**
 * This {@link AllocationDecider} control shard allocation by include and
 * exclude filters via dynamic cluster and index routing settings.
 * <p>
 * This filter is used to make explicit decision on which nodes certain shard
 * can / should be allocated. The decision if a shard can be allocated, must not
 * be allocated or should be allocated is based on either cluster wide dynamic
 * settings (<tt>cluster.routing.allocation.*</tt>) or index specific dynamic
 * settings (<tt>index.routing.allocation.*</tt>). All of those settings can be
 * changed at runtime via the cluster or the index update settings API.
 * </p>
 * Note: Cluster settings are applied first and will override index specific
 * settings such that if a shard can be allocated according to the index routing
 * settings it wont be allocated on a node if the cluster specific settings
 * would disallow the allocation. Filters are applied in the following order:
 * <ol>
 * <li><tt>required</tt> - filters required allocations. 
 * If any <tt>required</tt> filters are set the allocation is denied if the index is <b>not</b> in the set of <tt>required</tt> to allocate on the filtered node</li>
 * 
 * <li><tt>include</tt> - filters "allowed" allocations. 
 * If any <tt>include</tt> filters are set the allocation is denied if the index is <b>not</b> in the set of <tt>include</tt> filters for the filtered node</li>
 * 
 * <li><tt>exclude</tt> - filters "prohibited" allocations. 
 * If any <tt>exclude</tt> filters are set the allocation is denied if the index is in the set of <tt>exclude</tt> filters for the filtered node</li>
 * </ol>
 * 
 * 
 */
public class FilterAllocationDecider extends AllocationDecider {

    private final static String NAME = "filter";

    static {
        MetaData.addDynamicSettings(
                "cluster.routing.allocation.require.*",
                "cluster.routing.allocation.include.*",
                "cluster.routing.allocation.exclude.*"
        );
        IndexMetaData.addDynamicSettings(
                "index.routing.allocation.require.*",
                "index.routing.allocation.include.*",
                "index.routing.allocation.exclude.*"
        );
    }

    private final static Decision YES = Decision.yes(NAME, "All allocation filters are satisfied");
    private final static Decision NO = Decision.no(NAME, "Some allocation filters are not satisfied");
    private final static Decision YES_NO_FILTERS = Decision.yes(NAME, "No allocation filters are configured");

    private volatile DiscoveryNodeFilters clusterRequireFilters;
    private volatile DiscoveryNodeFilters clusterIncludeFilters;
    private volatile DiscoveryNodeFilters clusterExcludeFilters;

    @Inject
    public FilterAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(NAME, settings);
        ImmutableMap<String, String> requireMap = settings.getByPrefix("cluster.routing.allocation.require.").getAsMap();
        if (requireMap.isEmpty()) {
            clusterRequireFilters = null;
        } else {
            clusterRequireFilters = DiscoveryNodeFilters.buildFromKeyValue(AND, requireMap);
        }
        ImmutableMap<String, String> includeMap = settings.getByPrefix("cluster.routing.allocation.include.").getAsMap();
        if (includeMap.isEmpty()) {
            clusterIncludeFilters = null;
        } else {
            clusterIncludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, includeMap);
        }
        ImmutableMap<String, String> excludeMap = settings.getByPrefix("cluster.routing.allocation.exclude.").getAsMap();
        if (excludeMap.isEmpty()) {
            clusterExcludeFilters = null;
        } else {
            clusterExcludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, excludeMap);
        }
        nodeSettingsService.addListener(new ApplySettings());
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return allowed(shardRouting, node, allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return allowed(shardRouting, node, allocation);
    }

    private Decision allowed(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        boolean detailed = allocation.explanation().level().debugEnabled();
        DiscoveryNodeFilters.Match match = null;

        // only collect all the matching filters on a trace level
        Decision.Multi yesDecision = null;
        if (allocation.explanation().level().traceEnabled()) {
            yesDecision = new Decision.Multi(NAME);
        }

        boolean hasFilters = false;

        if (clusterExcludeFilters != null) {
            hasFilters = true;
            match = clusterExcludeFilters.match(node.node(), detailed);
            if (match.matches()) {
                return allocation.decisionDebug(NO, "filter (cluster exclude)", match.explanation(), match.explanationParams());
            }
            if (yesDecision != null) {
                yesDecision.add(Decision.yes("cluster exclude filter", match.explanation(), match.explanationParams()));
            }
        }

        if (clusterIncludeFilters != null) {
            hasFilters = true;
            match = clusterIncludeFilters.match(node.node(), detailed);
            if (!match.matches()) {
                return allocation.decisionDebug(NO, "filter (cluster include)", match.explanation(), match.explanationParams());
            }
            if (yesDecision != null) {
                yesDecision.add(Decision.yes("cluster include filter", match.explanation(), match.explanationParams()));
            }
        }

        if (clusterRequireFilters != null) {
            hasFilters = true;
            match = clusterRequireFilters.match(node.node(), detailed);
            if (!match.matches()) {
                return allocation.decisionDebug(NO, "filter (cluster required)", match.explanation(), match.explanationParams());
            }
            if (yesDecision != null) {
                yesDecision.add(Decision.yes("cluster required filter", match.explanation(), match.explanationParams()));
            }
        }

        IndexMetaData indexMd = allocation.routingNodes().metaData().index(shardRouting.index());
        if (indexMd.excludeFilters() != null) {
            hasFilters = true;
            match = indexMd.excludeFilters().match(node.node(), detailed);
            if (match.matches()) {
                return allocation.decisionDebug(NO, "filter (index exclude)", match.explanation(), match.explanationParams());
            }
            if (yesDecision != null) {
                yesDecision.add(Decision.yes("index exclude filter", match.explanation(), match.explanationParams()));
            }
        }
        if (indexMd.includeFilters() != null) {
            hasFilters = true;
            match = indexMd.includeFilters().match(node.node(), detailed);
            if (!match.matches()) {
                return allocation.decisionDebug(NO, "filter (index include)", match.explanation(), match.explanationParams());
            }
            if (yesDecision != null) {
                yesDecision.add(Decision.yes("index include filter", match.explanation(), match.explanationParams()));
            }
        }
        if (indexMd.requireFilters() != null) {
            hasFilters = true;
            match = indexMd.requireFilters().match(node.node(), detailed);
            if (!match.matches()) {
                return allocation.decisionDebug(NO, "filter (index required)", match.explanation(), match.explanationParams());
            }
            if (yesDecision != null) {
                yesDecision.add(Decision.yes("index required filter", match.explanation(), match.explanationParams()));
            }
        }

        if (!hasFilters) {
            return YES_NO_FILTERS;
        }

        if (yesDecision != null) {
            return yesDecision;
        }

        return YES;
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            ImmutableMap<String, String> requireMap = settings.getByPrefix("cluster.routing.allocation.require.").getAsMap();
            if (!requireMap.isEmpty()) {
                clusterRequireFilters = DiscoveryNodeFilters.buildFromKeyValue(AND, requireMap);
            }
            ImmutableMap<String, String> includeMap = settings.getByPrefix("cluster.routing.allocation.include.").getAsMap();
            if (!includeMap.isEmpty()) {
                clusterIncludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, includeMap);
            }
            ImmutableMap<String, String> excludeMap = settings.getByPrefix("cluster.routing.allocation.exclude.").getAsMap();
            if (!excludeMap.isEmpty()) {
                clusterExcludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, excludeMap);
            }
        }
    }
}
