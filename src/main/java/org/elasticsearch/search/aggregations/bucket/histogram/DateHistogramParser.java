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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.TimeZoneRounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentPushParser;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValueSourceParser;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.support.numeric.ValueParser;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.DateTimeZone;

import java.io.IOException;

/**
 *
 */
public class DateHistogramParser implements Aggregator.Parser {

    private static final ImmutableMap<String, DateTimeUnit> dateFieldUnits = MapBuilder.<String, DateTimeUnit>newMapBuilder()
                .put("year", DateTimeUnit.YEAR_OF_CENTURY)
                .put("1y", DateTimeUnit.YEAR_OF_CENTURY)
                .put("quarter", DateTimeUnit.QUARTER)
                .put("1q", DateTimeUnit.QUARTER)
                .put("month", DateTimeUnit.MONTH_OF_YEAR)
                .put("1M", DateTimeUnit.MONTH_OF_YEAR)
                .put("week", DateTimeUnit.WEEK_OF_WEEKYEAR)
                .put("1w", DateTimeUnit.WEEK_OF_WEEKYEAR)
                .put("day", DateTimeUnit.DAY_OF_MONTH)
                .put("1d", DateTimeUnit.DAY_OF_MONTH)
                .put("hour", DateTimeUnit.HOUR_OF_DAY)
                .put("1h", DateTimeUnit.HOUR_OF_DAY)
                .put("minute", DateTimeUnit.MINUTES_OF_HOUR)
                .put("1m", DateTimeUnit.MINUTES_OF_HOUR)
                .put("second", DateTimeUnit.SECOND_OF_MINUTE)
                .put("1s", DateTimeUnit.SECOND_OF_MINUTE)
                .immutableMap();

    @Override
    public String type() {
        return InternalDateHistogram.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        InternalParser internalParser = XContentPushParser.object(new InternalParser(aggregationName, context)).parse(parser);
        return new HistogramAggregator.Factory(aggregationName, internalParser.valuesSourceConfig, internalParser.rounding,
                internalParser.order, internalParser.keyed, internalParser.minDocCount, InternalDateHistogram.FACTORY);
    }

    private static class InternalParser extends XContentPushParser.AbstractCallback<InternalParser> {

        private static final ParseField TIMEZONE = new ParseField("time_zone");
        private static final ParseField PREZONE = new ParseField("pre_zone");
        private static final ParseField PREZONE_ADJUST_LARGE_INTERVAL = new ParseField("pre_zone_adjust_large_interval");
        private static final ParseField POSTZONE = new ParseField("post_zone");
        private static final ParseField PREOFFSET = new ParseField("pre_offset");
        private static final ParseField POSTOFFSET = new ParseField("post_offset");
        private static final ParseField MIN_DOC_COUNT = new ParseField("min_doc_count");

        private final String aggregationName;
        private final SearchContext context;
        private final ValueSourceParser<NumericValuesSource> valueSourceParser;

        ValuesSourceConfig<NumericValuesSource> valuesSourceConfig;
        boolean keyed = false;
        long minDocCount = 1;
        InternalOrder order = (InternalOrder) Histogram.Order.KEY_ASC;
        String interval = null;
        TimeZoneRounding rounding;
        boolean preZoneAdjustLargeInterval = false;
        DateTimeZone preZone = DateTimeZone.UTC;
        DateTimeZone postZone = DateTimeZone.UTC;
        String format = null;
        long preOffset = 0;
        long postOffset = 0;

        boolean inOrder = false;

        private InternalParser(String aggregationName, SearchContext context) {
            this.aggregationName = aggregationName;
            this.context = context;
            this.valueSourceParser = new ValueSourceParser<NumericValuesSource>(NumericValuesSource.class, context);
        }

        @Override
        public boolean on(XContentParser.Token token, XContentParser parser) throws IOException {
            boolean consumed = super.on(token, parser);
            return valueSourceParser.on(token, parser) || consumed;
        }

        @Override
        public InternalParser process() {

            if (interval == null) {
                throw new SearchParseException(context, "Missing required field [interval] for histogram aggregation [" + aggregationName + "]");
            }

            valuesSourceConfig = valueSourceParser.process();
            if (format != null) {
                valuesSourceConfig.formatter(new ValueFormatter.DateTime(format));
            }
            if (valuesSourceConfig.fieldContext() == null) {
                ValueParser valueParser = new ValueParser.DateMath(new DateMathParser(DateFieldMapper.Defaults.DATE_TIME_FORMATTER, DateFieldMapper.Defaults.TIME_UNIT));
                valuesSourceConfig.parser(valueParser);
            }

            TimeZoneRounding.Builder tzRoundingBuilder;
            DateTimeUnit dateTimeUnit = dateFieldUnits.get(interval);
            if (dateTimeUnit != null) {
                tzRoundingBuilder = TimeZoneRounding.builder(dateTimeUnit);
            } else {
                // the interval is a time value?
                tzRoundingBuilder = TimeZoneRounding.builder(TimeValue.parseTimeValue(interval, null));
            }

            rounding = tzRoundingBuilder
                    .preZone(preZone).postZone(postZone)
                    .preZoneAdjustLargeInterval(preZoneAdjustLargeInterval)
                    .preOffset(preOffset).postOffset(postOffset)
                    .build();

            return this;
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
            if (TIMEZONE.match(name) || PREZONE.match(name)) {
                if (isString(token)) {
                    preZone = parseZone(parser.text());
                } else if (isNumber(token)) {
                    preZone = DateTimeZone.forOffsetHours(parser.intValue());
                } else {
                    throw new SearchParseException(context, "[" + PREZONE.getPreferredName() + "] field in [" + aggregationName + "] must either be a string or an integer");
                }
                return true;
            }
            if (PREZONE_ADJUST_LARGE_INTERVAL.match(name)) {
                preZoneAdjustLargeInterval = parser.booleanValue();
                return true;
            }
            if (POSTZONE.match(name)) {
                if (isString(token)) {
                    postZone = parseZone(parser.text());
                } else if (isNumber(token)) {
                    postZone = DateTimeZone.forOffsetHours(parser.intValue());
                } else {
                    throw new SearchParseException(context, "[" + POSTZONE.getPreferredName() + "] field in [" + aggregationName + "] must either be a string or an integer");
                }
                return true;
            }
            if (PREOFFSET.match(name)) {
                preOffset = parseOffset(parser.text());
                return true;
            }
            if (POSTOFFSET.match(name)) {
                postOffset = parseOffset(parser.text());
                return true;
            }
            if ("interval".equals(name)) {
                interval = parser.text();
                return true;
            }
            if ("format".equals(name)) {
                format = parser.text();
                return true;
            }
            if ("keyed".equals(name)) {
                keyed = parser.booleanValue();
                return true;
            }
            if (MIN_DOC_COUNT.match(name)) {
                minDocCount = parser.longValue();
                return true;
            }
            return false;
        }

        @Override
        protected boolean onObjectStart(String name, XContentParser.Token token, XContentParser parser) throws IOException {
            if ("order".equals(name) && currentRoot()) {
                inOrder = true;
                return true;
            }
            return false;
        }

        @Override
        protected boolean onObjectEnd(String name, XContentParser.Token token, XContentParser parser) throws IOException {
            if ("order".equals(name) && currentRoot()) {
                inOrder = false;
                return true;
            }
            return false;
        }
    }

    private static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key) || "_time".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.KEY_ASC : InternalOrder.KEY_DESC);
        }
        if ("_count".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC);
        }
        return new InternalOrder.Aggregation(key, asc);
    }

    private static long parseOffset(String offset) throws IOException {
        if (offset.charAt(0) == '-') {
            return -TimeValue.parseTimeValue(offset.substring(1), null).millis();
        }
        int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue.parseTimeValue(offset.substring(beginIndex), null).millis();
    }

    private static DateTimeZone parseZone(String text) throws IOException {
        int index = text.indexOf(':');
        if (index != -1) {
            int beginIndex = text.charAt(0) == '+' ? 1 : 0;
            // format like -02:30
            return DateTimeZone.forOffsetHoursMinutes(
                    Integer.parseInt(text.substring(beginIndex, index)),
                    Integer.parseInt(text.substring(index + 1))
            );
        } else {
            // id, listed here: http://joda-time.sourceforge.net/timezones.html
            return DateTimeZone.forID(text);
        }
    }

}
