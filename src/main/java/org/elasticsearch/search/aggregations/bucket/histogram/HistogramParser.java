/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentPushParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValueSourceParser;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Parses the histogram request
 */
public class HistogramParser implements Aggregator.Parser {

    private static final ParseField MIN_DOC_COUNT = new ParseField("min_doc_count");

    @Override
    public String type() {
        return InternalHistogram.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        InternalParser internalParser = XContentPushParser.object(new InternalParser(aggregationName, context)).parse(parser);

        return new HistogramAggregator.Factory(aggregationName, internalParser.valuesSourceConfig, internalParser.rounding, internalParser.order,
                internalParser.keyed, internalParser.minDocCount, InternalHistogram.FACTORY);
    }

    private static class InternalParser extends XContentPushParser.AbstractCallback<InternalParser> {

        private final String aggregationName;
        private final SearchContext context;
        private final ValueSourceParser<NumericValuesSource> valueSourceParser;

        ValuesSourceConfig<NumericValuesSource> valuesSourceConfig;
        boolean keyed = false;
        long minDocCount = 1;
        InternalOrder order = (InternalOrder) InternalOrder.KEY_ASC;
        Rounding rounding = null;
        String format = null;

        boolean inOrder = false;

        private InternalParser(String aggregationName, SearchContext context) {
            this.aggregationName = aggregationName;
            this.context = context;
            this.valueSourceParser = new ValueSourceParser<NumericValuesSource>(NumericValuesSource.class, context);
        }

        @Override
        public InternalParser process() {
            valuesSourceConfig = valueSourceParser.process();

            if (rounding == null) {
                throw new SearchParseException(context, "Missing required field [interval] for histogram aggregation [" + aggregationName + "]");
            }

            return this;
        }

        @Override
        public boolean on(XContentParser.Token token, XContentParser parser) throws IOException {
            boolean consumed = super.on(token, parser);
            return valueSourceParser.on(token, parser) || consumed;
        }

        @Override
        protected boolean onValue(String name, XContentParser.Token token, XContentParser parser) throws IOException {
            if (inOrder) {
                order = resolveOrder(name, "asc".equals(parser.text()));
                return true;
            }
            if (!currentRoot()) {
                return false;
            }
            if ("format".equals(name)) {
                format = parser.text();
                return true;
            }
            if ("interval".equals(name)) {
                rounding = new Rounding.Interval(parser.longValue());
                return true;
            }
            if (MIN_DOC_COUNT.match(name)) {
                minDocCount = parser.longValue();
                return true;
            }
            if ("keyed".equals(name)) {
                keyed = parser.booleanValue();
                return true;
            }
            return false;
        }

        @Override
        protected boolean onObjectStart(String name, XContentParser.Token token, XContentParser parser) throws IOException {
            if (currentRoot() && "order".equals(name)) {
                inOrder = true;
                return true;
            }
            return false;
        }

        @Override
        protected boolean onObjectEnd(String name, XContentParser.Token token, XContentParser parser) throws IOException {
            if (currentRoot() && "order".equals(name)) {
                inOrder = false;
                return true;
            }
            return false;
        }
    }

    static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.KEY_ASC : InternalOrder.KEY_DESC);
        }
        if ("_count".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC);
        }
        return new InternalOrder.Aggregation(key, asc);
    }
}
