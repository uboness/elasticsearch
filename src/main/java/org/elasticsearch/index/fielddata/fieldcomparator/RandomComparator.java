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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;

/**
*
*/
public class RandomComparator extends FieldComparator<Long> {

    private final int[] values;
    private final long salt;

    private int docBase;
    private double bottom;

    public static IndexFieldData.XFieldComparatorSource comparatorSource(long seed, SearchShardTarget shardTarget) {
        return new InnerSource(seed, shardTarget);
    }

    static class InnerSource extends IndexFieldData.XFieldComparatorSource {

        private final long seed;
        private final SearchShardTarget shardTarget;

        public InnerSource(long seed, SearchShardTarget shardTarget) {
            this.seed = seed;
            this.shardTarget = shardTarget;
        }

        @Override
        public SortField.Type reducedType() {
            return SortField.Type.LONG;
        }

        @Override
        public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
            return new RandomComparator(numHits, seed, shardTarget);
        }

    }

    RandomComparator(int numHits, long seed, SearchShardTarget shardTarget) {
        values = new int[numHits];
        this.salt = salt(seed, shardTarget.index(), shardTarget.shardId());
    }

    @Override
    public int compare(int slot1, int slot2) {
        if (values[slot1] > values[slot2]) {
            return 1;
        }
        return values[slot1] < values[slot2] ? -1 : 0;
    }

    @Override
    public int compareBottom(int doc) {
        int value = hash(docBase + doc, salt);
        if (bottom > value) {
            return 1;
        }
        return bottom < value ? -1 : 0;
    }

    @Override
    public void copy(int slot, int doc) {
        values[slot] = hash(docBase + doc, salt);
    }

    @Override
    public FieldComparator<Long> setNextReader(AtomicReaderContext context) {
        this.docBase = context.docBase;
        return this;
    }

    @Override
    public void setBottom(int slot) {
        this.bottom = values[slot];
    }

    @Override
    public Long value(int slot) {
        return Long.valueOf(values[slot]);
    }

    @Override
    public int compareDocToValue(int doc, Long valueObj) {
        final long value = valueObj.longValue();
        long docValue = hash(docBase + doc, salt);
        if (docValue < value) {
            return -1;
        }
        return docValue > value ? 1 : 0;
    }

//    public static long hash(int doc, long salt) {
//        long x = doc + salt * 2057;
//        x ^= x << 32 ^ x >> 32;
//        x ^= (x << 21);
//        x ^= (x >>> 35);
//        x ^= (x << 4);
//        return (x < 0) ? -x : x;
//    }

    public static long salt(long seed, String index, int shardId) {
        long salt = index.hashCode();
        salt = salt << 32;
        salt |= shardId;
        return salt^seed;
    }


//    public float hash(int doc, long salt) {
//        int m = 0x5bd1e995;
//        int r = 24;
//        int h = seed ^ len;
//        for (int i = 0; i < 2; i++) {
//            int i_4 = offset + (i << 2);
//            int k = data[i_4 + 3];
//            k = k << 8;
//            k = k | (data[i_4 + 2] & 0xff);
//            k = k << 8;
//            k = k | (data[i_4 + 1] & 0xff);
//            k = k << 8;
//            k = k | (data[i_4 + 0] & 0xff);
//            k *= m;
//            k ^= k >>> r;
//            k *= m;
//            h *= m;
//            h ^= k;
//        }
//        int len_m = len_4 << 2;
//        int left = len - len_m;
//        if (left != 0) {
//            if (left >= 3) {
//                h ^= data[offset + len - 3] << 16;
//            }
//            if (left >= 2) {
//                h ^= data[offset + len - 2] << 8;
//            }
//            if (left >= 1) {
//                h ^= data[offset + len - 1];
//            }
//            h *= m;
//        }
//        h ^= h >>> 13;
//        h *= m;
//        h ^= h >>> 15;
//        return h;
//    }

    public static int hash(int doc, long salt) {

        long rand = doc;
        rand |= rand << 32;

//        int m = 0x5bd1e995;
        int m = (int) salt;
        int r = 24;

        int h = 0;

        int k = (int) rand * m;
        k ^= k >>> r;
        h ^= k * m;

        k = (int) (rand >> 32) * m;
        k ^= k >>> r;
        h *= m;
        h ^= k * m;

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }



}
