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

package org.elasticsearch.test.integration.search.facet.terms;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.*;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.facet.FacetBuilders.termsFacet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class UnmappedFieldsTermsFacetsTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", numberOfShards())
                .put("index.number_of_replicas", 0).build();
        for (int i = 0; i < numberOfNodes(); i++) {
            startNode("node" + i, settings);
        }
        client = getClient();
    }

    protected int numberOfShards() {
        return 5;
    }

    protected int numberOfNodes() {
        return 1;
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node0");
    }

    @AfterMethod
    public void cleanupTest() {
        client.admin().indices().prepareDelete("_all").execute().actionGet();
    }

    /**
     * Tests the terms facet when faceting on unmapped field
     */
    @Test
    public void testUnmappedField() throws Exception {

        client.admin().indices().prepareCreate("idx").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            client.prepareIndex("idx", "type", ""+i).setSource(jsonBuilder().startObject()
                    .field("mapped", ""+i)
                    .endObject()).execute().actionGet();
        }

        String[] execHints = new String[] {"map", null};
        for (String hint : execHints) {

            client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("mapped").executionHint(hint).field("mapped").size(10))
                    .addFacet(termsFacet("unmapped_bool").executionHint(hint).field("unmapped_bool").size(10))
                    .addFacet(termsFacet("unmapped_str").executionHint(hint).field("unmapped_str").size(10))
                    .addFacet(termsFacet("unmapped_byte").executionHint(hint).field("unmapped_byte").size(10))
                    .addFacet(termsFacet("unmapped_short").executionHint(hint).field("unmapped_short").size(10))
                    .addFacet(termsFacet("unmapped_int").executionHint(hint).field("unmapped_int").size(10))
                    .addFacet(termsFacet("unmapped_long").executionHint(hint).field("unmapped_long").size(10))
                    .addFacet(termsFacet("unmapped_float").executionHint(hint).field("unmapped_float").size(10))
                    .addFacet(termsFacet("unmapped_double").executionHint(hint).field("unmapped_double").size(10))
                    .execute().actionGet();

            assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

            // all values should be returned for the mapped field
            TermsFacet facet = searchResponse.getFacets().facet("mapped");
            assertThat(facet.getName(), equalTo("mapped"));
            assertThat(facet.getEntries().size(), is(10));
            assertThat(facet.getTotalCount(), is(10l));
            assertThat(facet.getMissingCount(), is(0l));

            // no values should be returned for the unmapped field (all docs are missing)

            facet = searchResponse.getFacets().facet("unmapped_str");
            assertThat(facet.getName(), equalTo("unmapped_str"));
            assertThat(facet.getEntries().size(), is(0));
            assertThat(facet.getTotalCount(), is(0l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("unmapped_bool");
            assertThat(facet.getName(), equalTo("unmapped_bool"));
            assertThat(facet.getEntries().size(), is(0));
            assertThat(facet.getTotalCount(), is(0l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("unmapped_byte");
            assertThat(facet.getName(), equalTo("unmapped_byte"));
            assertThat(facet.getEntries().size(), is(0));
            assertThat(facet.getTotalCount(), is(0l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("unmapped_short");
            assertThat(facet.getName(), equalTo("unmapped_short"));
            assertThat(facet.getEntries().size(), is(0));
            assertThat(facet.getTotalCount(), is(0l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("unmapped_int");
            assertThat(facet.getName(), equalTo("unmapped_int"));
            assertThat(facet.getEntries().size(), is(0));
            assertThat(facet.getTotalCount(), is(0l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("unmapped_long");
            assertThat(facet.getName(), equalTo("unmapped_long"));
            assertThat(facet.getEntries().size(), is(0));
            assertThat(facet.getTotalCount(), is(0l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("unmapped_float");
            assertThat(facet.getName(), equalTo("unmapped_float"));
            assertThat(facet.getEntries().size(), is(0));
            assertThat(facet.getTotalCount(), is(0l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("unmapped_double");
            assertThat(facet.getName(), equalTo("unmapped_double"));
            assertThat(facet.getEntries().size(), is(0));
            assertThat(facet.getTotalCount(), is(0l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));
        }

    }


    /**
     * Tests the terms facet when faceting on partially unmapped field. An example for this scenario is when searching
     * across indices, where the field is mapped in some indices and unmapped in others.
     */
    @Test
    public void testPartiallyUnmappedField() throws ElasticSearchException, IOException {

        client.admin().indices().prepareCreate("mapped_idx")
                .addMapping("type", jsonBuilder().startObject().startObject("type").startObject("properties")
                        .startObject("partially_mapped_byte").field("type", "byte").endObject()
                        .startObject("partially_mapped_short").field("type", "short").endObject()
                        .startObject("partially_mapped_int").field("type", "integer").endObject()
                        .startObject("partially_mapped_long").field("type", "long").endObject()
                        .startObject("partially_mapped_float").field("type", "float").endObject()
                        .startObject("partially_mapped_double").field("type", "double").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client.admin().indices().prepareCreate("unmapped_idx").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            client.prepareIndex("mapped_idx", "type", ""+i).setSource(jsonBuilder().startObject()
                    .field("mapped", "" + i)
                    .field("partially_mapped_str", ""+i)
                    .field("partially_mapped_bool", i%2 == 0)
                    .field("partially_mapped_byte", i)
                    .field("partially_mapped_short", i)
                    .field("partially_mapped_int", i)
                    .field("partially_mapped_long", i)
                    .field("partially_mapped_float", i)
                    .field("partially_mapped_double", i)
                    .endObject()).execute().actionGet();
        }

        for (int i = 10; i < 20; i++) {
            client.prepareIndex("unmapped_idx", "type", ""+i).setSource(jsonBuilder().startObject()
                    .field("mapped", ""+i)
                    .endObject()).execute().actionGet();
        }

        String[] execHint = new String[] {"map", null};
        for (String hint : execHint) {

            client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("mapped").executionHint(hint).field("mapped").size(10))
                    .addFacet(termsFacet("partially_mapped_str").executionHint(hint).field("partially_mapped_str").size(10))
                    .addFacet(termsFacet("partially_mapped_bool").executionHint(hint).field("partially_mapped_bool").size(10))
                    .addFacet(termsFacet("partially_mapped_byte").executionHint(hint).field("partially_mapped_byte").size(10))
                    .addFacet(termsFacet("partially_mapped_short").executionHint(hint).field("partially_mapped_short").size(10))
                    .addFacet(termsFacet("partially_mapped_int").executionHint(hint).field("partially_mapped_int").size(10))
                    .addFacet(termsFacet("partially_mapped_long").executionHint(hint).field("partially_mapped_long").size(10))
                    .addFacet(termsFacet("partially_mapped_float").executionHint(hint).field("partially_mapped_float").size(10))
                    .addFacet(termsFacet("partially_mapped_double").executionHint(hint).field("partially_mapped_double").size(10))
                    .execute().actionGet();

            assertThat(searchResponse.getHits().getTotalHits(), equalTo(20l));

            // all values should be returned for the mapped field
            TermsFacet facet = searchResponse.getFacets().facet("mapped");
            assertThat(facet.getName(), equalTo("mapped"));
            assertThat(facet.getEntries().size(), is(10));
            assertThat(facet.getTotalCount(), is(20l));
            assertThat(facet.getOtherCount(), is(10l));
            assertThat(facet.getMissingCount(), is(0l));

            // only the values of the mapped index should be returned for the partially mapped field (all docs of
            // the unmapped index should be missing)

            facet = searchResponse.getFacets().facet("partially_mapped_str");
            assertThat(facet.getName(), equalTo("partially_mapped_str"));
            assertThat(facet.getEntries().size(), is(10));
            assertThat(facet.getTotalCount(), is(10l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("partially_mapped_bool");
            assertThat(facet.getName(), equalTo("partially_mapped_bool"));
            assertThat(facet.getEntries().size(), is(2));
            assertThat(facet.getTotalCount(), is(10l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("partially_mapped_byte");
            assertThat(facet.getName(), equalTo("partially_mapped_byte"));
            assertThat(facet.getEntries().size(), is(10));
            assertThat(facet.getTotalCount(), is(10l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("partially_mapped_short");
            assertThat(facet.getName(), equalTo("partially_mapped_short"));
            assertThat(facet.getEntries().size(), is(10));
            assertThat(facet.getTotalCount(), is(10l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("partially_mapped_int");
            assertThat(facet.getName(), equalTo("partially_mapped_int"));
            assertThat(facet.getEntries().size(), is(10));
            assertThat(facet.getTotalCount(), is(10l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("partially_mapped_long");
            assertThat(facet.getName(), equalTo("partially_mapped_long"));
            assertThat(facet.getEntries().size(), is(10));
            assertThat(facet.getTotalCount(), is(10l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("partially_mapped_float");
            assertThat(facet.getName(), equalTo("partially_mapped_float"));
            assertThat(facet.getEntries().size(), is(10));
            assertThat(facet.getTotalCount(), is(10l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));

            facet = searchResponse.getFacets().facet("partially_mapped_float");
            assertThat(facet.getName(), equalTo("partially_mapped_float"));
            assertThat(facet.getEntries().size(), is(10));
            assertThat(facet.getTotalCount(), is(10l));
            assertThat(facet.getOtherCount(), is(0l));
            assertThat(facet.getMissingCount(), is(10l));
        }
    }
}
