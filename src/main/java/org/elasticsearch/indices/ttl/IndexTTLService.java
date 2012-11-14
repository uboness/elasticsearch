/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.ttl;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataSettingsProcessor;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A master node level service that purges expired indices. An index becomes expired iff it has a {@code index.ttl.purge_at}
 * settings, which defines the expiration date/time. This service periodically checks the cluster state and looks up indices which should
 * be expired. The interval in which this check is performed is defined by the dynamic node settings {@code indices.ttle.index_purge_interval}
 * (as a consequence of this nature, this service makes can only guarantee best effort in purging the expired indices on time). This
 * settings accepts either an absolute date value (ISO8601 date with optional time), or a relative date/time value which is
 * based on the {@link DateMathParser} (in the later case, "now" would indicate the date/time of the settings update or the index creation - depending
 * on when this settings is being processed)
 * <p/>
 * The index purging action can be one of the following: {@code delete} or {@code close}. By default, the action is set to {@code delete}
 * (meaning the index will be permanently deleted), but it can be configured using the index level dynamic setting
 * {@code index.ttl.index_purge_action} which takes either {@code delete} or {@code close} as values.
 */
public class IndexTTLService extends AbstractLifecycleComponent<IndexTTLService> {

    static {
        MetaData.addDynamicSettings(
                "indices.ttl.index_purge_interval"
        );

        IndexMetaData.addDynamicSettings(
                "index.ttl.purge_index_at",
                "index.ttl.index_purge_action"
        );

        IndexMetaData.addDynamicSettingsProcessor(new PurgeTimeProcessor());
    }

    private static DateMathParser dateParser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);

    public static enum PurgeAction {
        CLOSE, DELETE
    }

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final Client client;
    private final PurgeController purgeController;

    @Inject
    public IndexTTLService(Settings settings, ClusterService clusterService, IndicesService indicesService,
                           NodeSettingsService nodeSettingsService, Client client, ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.client = client;

        TimeValue interval = componentSettings.getAsTime("index_purge_interval", TimeValue.timeValueSeconds(60));
        this.purgeController = new PurgeController(threadPool, interval);

        clusterService.add(purgeController);
        nodeSettingsService.addListener(new ApplySettings());
    }

    @Override
    protected void doStart() throws ElasticSearchException {
    }

    @Override
    protected void doStop() throws ElasticSearchException {
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        purgeController.stop();
    }

    private class IndexPurger implements Runnable {

        public void run() {
            logger.info("Index purging process started...");

            MetaData metaData = clusterService.state().metaData();
            List<String> toBeDeleted = new ArrayList<String>();
            List<String> toBeClosed = new ArrayList<String>();
            for (IndexService indexService : indicesService) {
                IndexMetaData indexMetaData = metaData.index(indexService.index().name());
                Long purgeAt = indexMetaData.settings().getAsLong("index.ttl.purge_index_at", null);
                if (purgeAt != null && purgeAt < System.currentTimeMillis()) {
                    PurgeAction purgeAction = indexMetaData.settings().getAsEnum("index.ttl.purge_action", PurgeAction.class, PurgeAction.DELETE);
                    String indexName = indexService.index().name();
                    switch (purgeAction) {
                        case CLOSE:
                            toBeClosed.add(indexName);
                            break;

                        case DELETE:
                            toBeDeleted.add(indexName);
                    }
                }
            }

            for (String indexName : toBeClosed) {
                final String name = indexName;
                logger.info("TTL purging [CLOSING] index [" + name + "]");
                client.admin().indices().prepareClose(indexName).execute().addListener(new ActionListener<CloseIndexResponse>() {
                    @Override
                    public void onResponse(CloseIndexResponse closeIndexResponse) {
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.error("Failed to TTL CLOSE index [" + name + "]", e);
                    }
                });
            }

            // important check - empty collection will trigger an "all indices delete"
            if (!toBeDeleted.isEmpty()) {
                logger.info("TTL purging [DELETING] indices [" + Strings.collectionToCommaDelimitedString(toBeDeleted) + "]");
                client.admin().indices().prepareDelete(toBeDeleted.toArray(new String[toBeDeleted.size()])).execute()
                        .addListener(new ActionListener<DeleteIndexResponse>() {
                            @Override
                            public void onResponse(DeleteIndexResponse deleteIndexResponse) {
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                logger.error("Failed to TTL DELETE indices", e);
                            }
                        });
            }
        }
    }

    class PurgeController implements LocalNodeMasterListener {

        private final ThreadPool threadPool;
        private final IndexPurger purger = new IndexPurger();
        private TimeValue interval;
        private ScheduledFuture taskFuture;

        PurgeController(ThreadPool threadPool, TimeValue interval) {
            this.threadPool = threadPool;
            this.interval = interval;
        }

        public synchronized void start() {
            taskFuture = threadPool.scheduleWithFixedDelay(purger, interval);
        }

        public synchronized void updateInterval(TimeValue interval) {
            if (this.interval.equals(interval)) {
                return;
            }
            logger.info("updating indices.ttl.interval from [{}] to [{}]", this.interval, interval);
            this.interval = interval;
            stop();
            start();
        }

        public synchronized void stop() {
            if (taskFuture != null) {
                taskFuture.cancel(true);
            }
        }

        @Override
        public void onMaster() {
            start();
        }

        @Override
        public void offMaster() {
            stop();
        }

        @Override
        public String executorName() {
            return ThreadPool.Names.SAME;
        }
    }

    class ApplySettings implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            TimeValue interval = settings.getAsTime("indices.ttl.index_purge_interval", null);
            if (interval != null) {
                IndexTTLService.this.purgeController.updateInterval(interval);
            }
        }
    }

    /**
     * Converts {@code index.ttl.purge_index_at} from a datetime or date math format to absolute milliseconds.
     */
    static class PurgeTimeProcessor implements MetaDataSettingsProcessor {

        @Override
        public void process(ImmutableSettings.Builder settingsBuilder) {
            String dateString = settingsBuilder.get("index.ttl.purge_index_at");
            if (dateString != null) {
                try {
                    long millis = dateParser.parse(dateString, System.currentTimeMillis());
                    settingsBuilder.put("index.ttl.purge_index_at", millis);
                } catch (ElasticSearchParseException e) {
                    throw new SettingsException("Could not parse [index.ttl.purge_index_at] as absolute or relative datetime", e);
                }
            }
        }
    }
}