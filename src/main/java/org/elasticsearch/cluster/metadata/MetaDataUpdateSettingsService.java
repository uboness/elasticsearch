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

package org.elasticsearch.cluster.metadata;

import com.google.common.collect.Sets;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;

/**
 *
 */
public class MetaDataUpdateSettingsService extends AbstractComponent implements ClusterStateListener {

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    @Inject
    public MetaDataUpdateSettingsService(Settings settings, ClusterService clusterService, AllocationService allocationService) {
        super(settings);
        this.clusterService = clusterService;
        this.clusterService.add(this);
        this.allocationService = allocationService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // update an index with number of replicas based on data nodes if possible
        if (!event.state().nodes().localNodeMaster()) {
            return;
        }
        // we need to do this each time in case it was changed by update settings
        for (final IndexMetaData indexMetaData : event.state().metaData()) {
            String autoExpandReplicas = indexMetaData.settings().get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS);
            if (autoExpandReplicas != null && Booleans.parseBoolean(autoExpandReplicas, true)) { // Booleans only work for false values, just as we want it here
                try {
                    int min;
                    int max;
                    try {
                        min = Integer.parseInt(autoExpandReplicas.substring(0, autoExpandReplicas.indexOf('-')));
                        String sMax = autoExpandReplicas.substring(autoExpandReplicas.indexOf('-') + 1);
                        if (sMax.equals("all")) {
                            max = event.state().nodes().dataNodes().size() - 1;
                        } else {
                            max = Integer.parseInt(sMax);
                        }
                    } catch (Exception e) {
                        logger.warn("failed to set [{}], wrong format [{}]", e, IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, autoExpandReplicas);
                        continue;
                    }

                    int numberOfReplicas = event.state().nodes().dataNodes().size() - 1;
                    if (numberOfReplicas < min) {
                        numberOfReplicas = min;
                    } else if (numberOfReplicas > max) {
                        numberOfReplicas = max;
                    }

                    // same value, nothing to do there
                    if (numberOfReplicas == indexMetaData.numberOfReplicas()) {
                        continue;
                    }

                    if (numberOfReplicas >= min && numberOfReplicas <= max) {
                        final int fNumberOfReplicas = numberOfReplicas;
                        Settings settings = ImmutableSettings.settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, fNumberOfReplicas).build();
                        updateSettings(settings, new String[]{indexMetaData.index()}, new Listener() {
                            @Override
                            public void onSuccess() {
                                logger.info("[{}] auto expanded replicas to [{}]", indexMetaData.index(), fNumberOfReplicas);
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                logger.warn("[{}] fail to auto expand replicas to [{}]", indexMetaData.index(), fNumberOfReplicas);
                            }
                        });
                    }
                } catch (Exception e) {
                    logger.warn("[{}] failed to parse auto expand replicas", e, indexMetaData.index());
                }
            }
        }
    }

    public void updateSettings(final Settings pSettings, final String[] indices, final Listener listener) {
        ImmutableSettings.Builder updatedSettingsBuilder = ImmutableSettings.settingsBuilder();
        for (Map.Entry<String, String> entry : pSettings.getAsMap().entrySet()) {
            if (entry.getKey().equals("index")) {
                continue;
            }
            if (!entry.getKey().startsWith("index.")) {
                updatedSettingsBuilder.put("index." + entry.getKey(), entry.getValue());
            } else {
                updatedSettingsBuilder.put(entry.getKey(), entry.getValue());
            }
        }
        // never allow to change the number of shards
        for (String key : updatedSettingsBuilder.internalMap().keySet()) {
            if (key.equals(IndexMetaData.SETTING_NUMBER_OF_SHARDS)) {
                listener.onFailure(new ElasticSearchIllegalArgumentException("can't change the number of shards for an index"));
                return;
            }
        }

        final Settings closeSettings = updatedSettingsBuilder.build();

        final Set<String> removedSettings = Sets.newHashSet();
        for (String key : updatedSettingsBuilder.internalMap().keySet()) {
            if (!IndexMetaData.hasDynamicSetting(key)) {
                removedSettings.add(key);
            }
        }
        if (!removedSettings.isEmpty()) {
            for (String removedSetting : removedSettings) {
                updatedSettingsBuilder.remove(removedSetting);
            }
        }
        final Settings openSettings = IndexMetaData.process(updatedSettingsBuilder).build();

        clusterService.submitStateUpdateTask("update-settings", new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                try {
                    String[] actualIndices = currentState.metaData().concreteIndices(indices);
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder().routingTable(currentState.routingTable());
                    MetaData.Builder metaDataBuilder = MetaData.newMetaDataBuilder().metaData(currentState.metaData());

                    int updatedNumberOfReplicas = openSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, -1);
                    if (updatedNumberOfReplicas != -1) {
                        routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                        metaDataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                        logger.info("updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas, actualIndices);
                    }

                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    Boolean updatedReadOnly = openSettings.getAsBoolean(IndexMetaData.SETTING_READ_ONLY, null);
                    if (updatedReadOnly != null) {
                        for (String index : actualIndices) {
                            if (updatedReadOnly) {
                                blocks.addIndexBlock(index, IndexMetaData.INDEX_READ_ONLY_BLOCK);
                            } else {
                                blocks.removeIndexBlock(index, IndexMetaData.INDEX_READ_ONLY_BLOCK);
                            }
                        }
                    }
                    Boolean updateMetaDataBlock = openSettings.getAsBoolean(IndexMetaData.SETTING_BLOCKS_METADATA, null);
                    if (updateMetaDataBlock != null) {
                        for (String index : actualIndices) {
                            if (updateMetaDataBlock) {
                                blocks.addIndexBlock(index, IndexMetaData.INDEX_METADATA_BLOCK);
                            } else {
                                blocks.removeIndexBlock(index, IndexMetaData.INDEX_METADATA_BLOCK);
                            }
                        }
                    }

                    Boolean updateWriteBlock = openSettings.getAsBoolean(IndexMetaData.SETTING_BLOCKS_WRITE, null);
                    if (updateWriteBlock != null) {
                        for (String index : actualIndices) {
                            if (updateWriteBlock) {
                                blocks.addIndexBlock(index, IndexMetaData.INDEX_WRITE_BLOCK);
                            } else {
                                blocks.removeIndexBlock(index, IndexMetaData.INDEX_WRITE_BLOCK);
                            }
                        }
                    }

                    Boolean updateReadBlock = openSettings.getAsBoolean(IndexMetaData.SETTING_BLOCKS_READ, null);
                    if (updateReadBlock != null) {
                        for (String index : actualIndices) {
                            if (updateReadBlock) {
                                blocks.addIndexBlock(index, IndexMetaData.INDEX_READ_BLOCK);
                            } else {
                                blocks.removeIndexBlock(index, IndexMetaData.INDEX_READ_BLOCK);
                            }
                        }
                    }

                    // allow to change any settings to a close index, and only allow dynamic settings to be changed
                    // on an open index
                    Set<String> openIndices = Sets.newHashSet();
                    Set<String> closeIndices = Sets.newHashSet();
                    for (String index : actualIndices) {
                        if (currentState.metaData().index(index).state() == IndexMetaData.State.OPEN) {
                            openIndices.add(index);
                        } else {
                            closeIndices.add(index);
                        }
                    }

                    if (!openIndices.isEmpty()) {
                        String[] indices = openIndices.toArray(new String[openIndices.size()]);
                        if (!removedSettings.isEmpty()) {
                            logger.warn("{} ignoring non dynamic index level settings for open indices: {}", indices, removedSettings);
                        }
                        metaDataBuilder.updateSettings(openSettings, indices);
                    }

                    if (!closeIndices.isEmpty()) {
                        String[] indices = closeIndices.toArray(new String[closeIndices.size()]);
                        metaDataBuilder.updateSettings(closeSettings, indices);
                    }


                    ClusterState updatedState = ClusterState.builder().state(currentState).metaData(metaDataBuilder).routingTable(routingTableBuilder).blocks(blocks).build();

                    // now, reroute in case things change that require it (like number of replicas)
                    RoutingAllocation.Result routingResult = allocationService.reroute(updatedState);
                    updatedState = newClusterStateBuilder().state(updatedState).routingResult(routingResult).build();

                    return updatedState;
                } catch (Exception e) {
                    listener.onFailure(e);
                    return currentState;
                }
            }

            @Override
            public void clusterStateProcessed(ClusterState clusterState) {
                listener.onSuccess();
            }
        });
    }

    public static interface Listener {
        void onSuccess();

        void onFailure(Throwable t);
    }
}
