package com.ivy.transportclient.aggregation;

import java.io.IOException;
import java.util.Date;

import com.ivy.transportclient.EsStartStop;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;

public class MetricsAggregation {
    public static void main(String[] args) throws IOException {
        EsStartStop esStartStop = new EsStartStop();
        TransportClient client = esStartStop.getClient();

        /////////////////////////// data
        IndexResponse indexResponse = client.prepareIndex("twitter1", "tweet", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("height", 11)
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject())
                .get();

        IndexResponse indexResponse1 = client.prepareIndex("twitter1", "tweet", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("height", 6)
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject())
                .get();

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("twitter1").setTypes("tweet");
        /////////////////////////// min
        MinAggregationBuilder minAggregationBuilder = AggregationBuilders .min("minHeight").field("height");
        searchRequestBuilder.addAggregation(minAggregationBuilder);
        Min minHeight = searchRequestBuilder.get().getAggregations().get("minHeight");
        double minHeightValue = minHeight.getValue();
        System.out.println(minHeightValue);

        /////////////////////////// max
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("maxHeight").field("height");
        searchRequestBuilder.addAggregation(maxAggregationBuilder);
        Max maxHeight = searchRequestBuilder.get().getAggregations().get("maxHeight");
        double maxHeightValue = maxHeight.getValue();
        System.out.println(maxHeightValue);

        /////////////////////////// max
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("sumHeight").field("height");
        searchRequestBuilder.addAggregation(sumAggregationBuilder);
        Sum sumHeight = searchRequestBuilder.get().getAggregations().get("sumHeight");
        double sumHeightValue = sumHeight.getValue();
        System.out.println(sumHeightValue);

        /////////////////////////// avg
        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg("avgHeight").field("height");
        searchRequestBuilder.addAggregation(avgAggregationBuilder);
        Avg avgHeight = searchRequestBuilder.get().getAggregations().get("avgHeight");
        double avgHeightValue = avgHeight.getValue();
        System.out.println(avgHeightValue);

        /////////////////////////// count
        ValueCountAggregationBuilder valueCountAggregationBuilder = AggregationBuilders.count("countHeight").field("height");
        searchRequestBuilder.addAggregation(valueCountAggregationBuilder);
        ValueCount countHeight = searchRequestBuilder.get().getAggregations().get("countHeight");
        long countHeightValue = countHeight.getValue();
        System.out.println(countHeightValue);

        /////////////////////////// Stats
        StatsAggregationBuilder statsAggregationBuilder = AggregationBuilders.stats("statsHeight").field("height");
        searchRequestBuilder.addAggregation(statsAggregationBuilder);
        Stats statsHeight = searchRequestBuilder.get().getAggregations().get("statsHeight");
        double min = statsHeight.getMin();
        double max = statsHeight.getMax();
        double avg = statsHeight.getAvg();
        double sum = statsHeight.getSum();
        long count = statsHeight.getCount();
        System.out.println("min---------" + min
                + "------max------" + max
                + "------avg------" + avg
                + "------sum------" + sum
                + "------count------" + count);

        /////////////////////////// extended Stats
        ExtendedStatsAggregationBuilder extendedStatsAggregationBuilder = AggregationBuilders.extendedStats("extendedStatsHeight").field("height");
        searchRequestBuilder.addAggregation(extendedStatsAggregationBuilder);
        ExtendedStats extendedStatsHeight = searchRequestBuilder.get().getAggregations().get("extendedStatsHeight");
        min = extendedStatsHeight.getMin();
        max = extendedStatsHeight.getMax();
        avg = extendedStatsHeight.getAvg();
        sum = extendedStatsHeight.getSum();
        count = extendedStatsHeight.getCount();
        // 标准差
        double stdDeviation = extendedStatsHeight.getStdDeviation();
        // 平方和
        double sumOfSquares = extendedStatsHeight.getSumOfSquares();
        // 方差
        double variance = extendedStatsHeight.getVariance();
        System.out.println("min---------" + min
                + "------max------" + max
                + "------avg------" + avg
                + "------sum------" + sum
                + "------count------" + count
                + "------stdDeviation------" + stdDeviation
                + "------sumOfSquares------" + sumOfSquares
                + "------variance------" + variance);

        /////////////////////////// Percentile
        PercentilesAggregationBuilder percentilesAggregationBuilder = AggregationBuilders
                .percentiles("percentileHeight")
                .field("height")
                // You can provide your own percentiles instead of using defaults
                .percentiles(1.0, 5.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0);

        searchRequestBuilder.addAggregation(percentilesAggregationBuilder);
        Percentiles percentileHeight = searchRequestBuilder.get().getAggregations().get("percentileHeight");
        for (Percentile entry : percentileHeight) {
            double percent = entry.getPercent();    // Percent
            double value = entry.getValue();        // Value

            System.out.println("percent = " + percent + ", value = " + value);
        }

        /////////////////////////// Percentile rank
        PercentileRanksAggregationBuilder percentileRanksAggregationBuilder = AggregationBuilders
                .percentileRanks("percentileRankHeight")
                .field("height")
                .values(1.24, 1.91, 2.22);

        searchRequestBuilder.addAggregation(percentileRanksAggregationBuilder);
        PercentileRanks percentileRankHeight = searchRequestBuilder.get().getAggregations().get("percentileRankHeight");
        for (Percentile entry : percentileRankHeight) {
            double percent = entry.getPercent();    // Percent
            double value = entry.getValue();        // Value

            System.out.println("percent = " + percent + ", value = " + value);
        }

        /////////////////////////// Cardinality
        /////////////////////////// Geo Bounds
        /////////////////////////// top Hits
        /////////////////////////// Scripted Metric


        ////////////////////////// close
        esStartStop.close(client);
    }

}
