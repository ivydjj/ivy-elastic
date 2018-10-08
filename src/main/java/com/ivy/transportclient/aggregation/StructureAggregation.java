package com.ivy.transportclient.aggregation;

import java.io.IOException;

import com.ivy.transportclient.EsStartStop;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

/*
？？？？？？？？？？
 */
public class StructureAggregation {
    public static void main(String[] args) throws IOException {
        EsStartStop esStartStop = new EsStartStop();
        TransportClient client = esStartStop.getClient();

        ///////////////////////////

        /*
        here is a 3 levels aggregation composed of:

        Terms aggregation (bucket)
        Date Histogram aggregation (bucket)
        Average aggregation (metric)
         */
        SearchResponse searchResponse = client.prepareSearch("twitter")
                .setTypes("tweet")
                .addAggregation(
                        AggregationBuilders.terms("by_country").field("country")
                                .subAggregation(AggregationBuilders.dateHistogram("by_year")
                                        .field("dateOfBirth")
                                        .dateHistogramInterval(DateHistogramInterval.YEAR)
                                        .subAggregation(AggregationBuilders.avg("avg_children").field("children"))
                                )
                )
                .execute().actionGet();

        System.out.println(searchResponse.getHits().getHits().length);
        searchResponse.getHits().forEach(e -> System.out.println(e.getSourceAsString()));

        ////////////////////////// close
        esStartStop.close(client);
    }

}
