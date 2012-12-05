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

package org.elasticsearch.test.integration.ttl;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class IndexTTLTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1", settingsBuilder()
                .put("indices.ttl.index_purge_interval", TimeValue.timeValueSeconds(1).toString())
                .build());
        client = client("node1");
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @Test
    public void testIndexTTL() throws Exception {

        client.admin().indices().prepareCreate("index1").setSettings(settingsBuilder()
                .put("index.ttl.purge_index_at", "now+100ms")
                .build()).execute().actionGet();

        assertThat(client.admin().indices().prepareExists("index1").execute().actionGet().exists(), is(true));

        // we need to sleep at least 1100ms (to let the purger thread kick in), let's make it 2000ms just to be sure.
        Thread.sleep(2000);

        assertThat(client.admin().indices().prepareExists("index1").execute().actionGet().exists(), is(false));


        // creating an index which is not purged by default.
        client.admin().indices().prepareCreate("index1").execute().actionGet().acknowledged();
        assertThat(client.admin().indices().prepareExists("index1").execute().actionGet().exists(), is(true));

        // let's first make sure it's not purged after the first run of the purger
        Thread.sleep(2000);
        assertThat(client.admin().indices().prepareExists("index1").execute().actionGet().exists(), is(true));


        // now setting it to purge after 1 second (this time we'll use absolute dates instead of date math)

        String purgeAtSetting = Joda.forPattern("dateOptionalTime").printer().print(new DateTime().plus(1000));

        client.admin().indices().prepareUpdateSettings("index1")
                .setSettings(settingsBuilder().put("index.ttl.purge_index_at", purgeAtSetting))
                .execute().actionGet();

        Thread.sleep(3000);
        assertThat(client.admin().indices().prepareExists("index1").execute().actionGet().exists(), is(false));
    }
}
