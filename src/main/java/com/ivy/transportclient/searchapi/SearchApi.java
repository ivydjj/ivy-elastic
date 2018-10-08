package com.ivy.transportclient.searchapi;

import java.io.IOException;

import com.ivy.transportclient.EsStartStop;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;

public class SearchApi {
    public static void main(String[] args) throws IOException {
        EsStartStop esStartStop = new EsStartStop();
        TransportClient client = esStartStop.getClient();

        ///////////////////////////
        // all parameters are optional
        SearchResponse response = client.prepareSearch("index1", "index2")
                .setTypes("type1", "type2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("multi", "test"))                 // Query
                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFrom(0).setSize(60).setExplain(true)
                .get();

        // MatchAll on the whole cluster with all default options
        SearchResponse response1 = client.prepareSearch().get();

        ////////////////////////// close
        esStartStop.close(client);
    }

}
