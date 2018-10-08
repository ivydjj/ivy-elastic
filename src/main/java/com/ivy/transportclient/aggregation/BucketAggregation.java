package com.ivy.transportclient.aggregation;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ivy.transportclient.EsStartStop;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

/*
 */
public class BucketAggregation {
    public static void main(String[] args) throws IOException {
        EsStartStop esStartStop = new EsStartStop();
        TransportClient client = esStartStop.getClient();

        /////////////////////////// data
//        DeleteIndexResponse deleteResponse = client.admin().indices().prepareDelete("twitter2").get();
//        if (!deleteResponse.isAcknowledged()){
//            throw new RuntimeException("删除Elasticsearch Index失败");
//        }
//
//        CreateIndexResponse createIndexResponse = client.admin().indices()
//                .prepareCreate("twitter2")
//                .get();
//
//        if (!createIndexResponse.isAcknowledged()){
//            throw new RuntimeException("创建Elasticsearch Index失败");
//        }
//        PutMappingResponse putMappingResponse = client.admin().indices()
//                .preparePutMapping("twitter2")
//                .setType("tweet")
//                .setSource(FileUtil.getFileAsString("user_mapping.json", BucketAggregation.class), XContentType.JSON).get();
//        if (!putMappingResponse.isAcknowledged()){
//            throw new RuntimeException("创建Elasticsearch Index Mapping失败");
//        }

        BucketModel bucketModel1 = new BucketModel("name1", 30, new BucketNestModel(30));
        BucketModel bucketModel2 = new BucketModel("name2", 2, new BucketNestModel(2));
        BucketModel bucketModel3 = new BucketModel("name3", 3, new BucketNestModel(3));
        BucketModel bucketModel4 = new BucketModel("name4", 4, new BucketNestModel(4));
        BucketModel bucketModel5 = new BucketModel("name5", 5, new BucketNestModel(5));
        BucketModel bucketModel6 = new BucketModel("name6", 6, new BucketNestModel(6));
        BucketModel bucketModel7 = new BucketModel("name7", 7, new BucketNestModel(7));
        BucketModel bucketModel8 = new BucketModel("name8", 8, new BucketNestModel(8));
        BucketModel bucketModel9 = new BucketModel("name9", 9, new BucketNestModel(9));
        BucketModel bucketModel10 = new BucketModel("name10", 10, new BucketNestModel(10));
        BucketModel bucketModel11 = new BucketModel("name11", 11, new BucketNestModel(11));
        BucketModel bucketModel12 = new BucketModel("name12", 12, new BucketNestModel(12));

        client.prepareIndex("twitter2", "tweet", "1").setSource(objToJson(bucketModel1), XContentType.JSON).get();
        client.prepareIndex("twitter2", "tweet", "2").setSource(objToJson(bucketModel2), XContentType.JSON).get();
        client.prepareIndex("twitter2", "tweet", "3").setSource(objToJson(bucketModel3), XContentType.JSON).get();
        client.prepareIndex("twitter2", "tweet", "4").setSource(objToJson(bucketModel4), XContentType.JSON).get();
        client.prepareIndex("twitter2", "tweet", "5").setSource(objToJson(bucketModel5), XContentType.JSON).get();
        client.prepareIndex("twitter2", "tweet", "6").setSource(objToJson(bucketModel6), XContentType.JSON).get();
        client.prepareIndex("twitter2", "tweet", "7").setSource(objToJson(bucketModel7), XContentType.JSON).get();
        client.prepareIndex("twitter2", "tweet", "8").setSource(objToJson(bucketModel8), XContentType.JSON).get();
        client.prepareIndex("twitter2", "tweet", "9").setSource(objToJson(bucketModel9), XContentType.JSON).get();
        client.prepareIndex("twitter2", "tweet", "10").setSource(objToJson(bucketModel10), XContentType.JSON).get();
        client.prepareIndex("twitter2", "tweet", "11").setSource(objToJson(bucketModel11), XContentType.JSON).get();
        client.prepareIndex("twitter2", "tweet", "12").setSource(objToJson(bucketModel12), XContentType.JSON).get();

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("twitter2").setTypes("tweet");

        /////////////////////////// global
        searchRequestBuilder.addAggregation(AggregationBuilders
                .global("globalAgg")
                .subAggregation(AggregationBuilders.terms("agg_name").field("height")));
        Global globalAgg = searchRequestBuilder.get().getAggregations().get("globalAgg");

        System.out.println(globalAgg.getDocCount());

        /////////////////////////// filter
        searchRequestBuilder.addAggregation(AggregationBuilders
                .filter("filterAgg", QueryBuilders.termQuery("height", 10)));
        Filter filterAgg = searchRequestBuilder.get().getAggregations().get("filterAgg");
        System.out.println(filterAgg.getDocCount());

        /////////////////////////// filters
        searchRequestBuilder.addAggregation(AggregationBuilders
                .filters("multiFilterAgg",
                        new FiltersAggregator.KeyedFilter("height", QueryBuilders.termQuery("height", 10)),
                        new FiltersAggregator.KeyedFilter("user", QueryBuilders.termQuery("name", "name2"))));

        Filters multiFilterAgg = searchRequestBuilder.get().getAggregations().get("multiFilterAgg");
        for (Filters.Bucket entry : multiFilterAgg.getBuckets()) {
            String key = entry.getKeyAsString();            // bucket key
            long docCount = entry.getDocCount();            // Doc count

            System.out.println("key = " + key + ", doc_count = " + docCount);
        }

        /////////////////////////// nested
        searchRequestBuilder.addAggregation(AggregationBuilders.nested("nestedAgg", "nestModel"));
        Nested nestedAgg = searchRequestBuilder.get().getAggregations().get("nestedAgg");
        System.out.println(nestedAgg.getDocCount());

        searchRequestBuilder
                // 加query默认返回5条数据
                .setQuery(QueryBuilders.rangeQuery("height").gte(2).lte(12))
                // nested: QueryPhaseExecutionException[Result window is too large, from + size must be less than or equal to: [10000] but was [100000].
                // See the scroll api for a more efficient way to request large data sets. This limit can be set by changing the [index.max_result_window] index level setting.
                .setFrom(0).setSize(10000) // 需放在聚合前
                .addAggregation(AggregationBuilders.nested("nestedAgg1", "nestModel")
                .subAggregation(AggregationBuilders.max("maxNestHeight").field("nestModel.nestHeight"))
                .subAggregation(AggregationBuilders.sum("sumNestHeight").field("nestModel.nestHeight")));

        Nested nestedAgg1 = searchRequestBuilder.get().getAggregations().get("nestedAgg1");
        Max nestedMax = nestedAgg1.getAggregations().get("maxNestHeight");
        System.out.println("--------------" + nestedMax.getValue());
        Sum nestedSum = nestedAgg1.getAggregations().get("sumNestHeight");
        System.out.println("--------------" + nestedSum.getValue());

        /////////////////////////// reverse nested
        AggregationBuilder aggregation =
                AggregationBuilders
                        .nested("reverseNestedAgg", "nestModel")
                        .subAggregation(
                                AggregationBuilders
                                        .terms("nestHeight").field("nestModel.nestHeight")
                                        .subAggregation(AggregationBuilders.reverseNested("nest_Height"))
                        );

        Nested reverseNestedAgg = searchRequestBuilder.addAggregation(aggregation).get().getAggregations().get("reverseNestedAgg");
        Terms name = reverseNestedAgg.getAggregations().get("nestHeight");
        for (Terms.Bucket bucket : name.getBuckets()) {
            ReverseNested nestHeight = bucket.getAggregations().get("nest_Height");
            // Doc count
            System.out.println(nestHeight.getDocCount());
        }





        ////////////////////////// close
        esStartStop.close(client);
    }

    public static String objToJson(Object obj){
        if (obj == null){
            return null;
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
