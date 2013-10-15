/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ScheduledIndexManagementService extends AbstractLifecycleComponent<ScheduledIndexManagementService> {

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final Client client;

    @Inject
    public ScheduledIndexManagementService(Settings settings, ClusterService clusterService, IndicesService indicesService, Client client) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.client = client;
    }

    @Override
    protected void doStart() throws ElasticSearchException {

    }

    @Override
    protected void doStop() throws ElasticSearchException {

    }

    @Override
    protected void doClose() throws ElasticSearchException {

    }

    class ManagementThread extends Thread {

        volatile boolean running = false;

        public void kill() {
            running = false;
        }

        @Override
        public void run() {
            while (running) {
                MetaData metaData = clusterService.state().metaData();
                for (IndexService indexService : indicesService) {


                }
            }
        }
    }

    static interface ScheduledIndexOperation {


        static interface Callback {

        }

    }

    class MasterListener implements LocalNodeMasterListener {

        @Override
        public void onMaster() {
        }

        @Override
        public void offMaster() {
        }

        @Override
        public String executorName() {
            return ThreadPool.Names.SAME;
        }
    }

    static interface IndexAction {

        void execute(Client client, MetaData metaData, IndicesService indexServices);

    }

    static class DeleteAction implements IndexAction {

        private final static Long NO_TTL = -1l;

        private final static String INDICES_TTL_DELETE = "indices.schedule.delete";
        private final static String INDEX_TTL_DELETE = "indices.schedule.delete";

        @Override
        public void execute(Client client, MetaData metaData, IndicesService indicesService) {
            List<String> toBeDeleted = new ArrayList<String>();
            for (IndexService indexService : indicesService) {
                Settings settings = indexService.settingsService().getSettings();
                long ttl = settings.getAsLong(INDEX_TTL_DELETE, NO_TTL);
                if (ttl == NO_TTL) {
                    settings = metaData.index(indexService.index().name()).settings();
                    ttl = settings.getAsLong(INDEX_TTL_DELETE, NO_TTL);
                    if (ttl == NO_TTL) {
                        settings = metaData.settings();
                        ttl = settings.getAsLong(INDICES_TTL_DELETE, NO_TTL);
                        if (ttl == NO_TTL) {
                            continue;
                        }
                    }
                }
                if (System.currentTimeMillis() > ttl) {
                    continue;
                }
                toBeDeleted.add(indexService.index().getName());
            }

            client.admin().indices().prepareDelete(toBeDeleted.toArray(new String[toBeDeleted.size()]))
                    .setReason("scheduled delete").execute().actionGet();
        }
    }
}
