package com.ivy.transportclient;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;

public class ReindexApi {
    public static void main(String[] args) throws IOException {
        EsStartStop esStartStop = new EsStartStop();
        TransportClient client = esStartStop.getClient();

        /////////////////////////// reindex api

        IndexResponse indexResponse = client.prepareIndex("twitter", "tweet", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject())
                .get();

        // 异常：org.elasticsearch.action.ActionRequestValidationException: Validation Failed: 1: use _all if you really want to copy from all existing indexes;
        BulkByScrollResponse response = ReindexAction.INSTANCE.newRequestBuilder(client)
                .destination("twitter")
                .filter(QueryBuilders.matchQuery("category", "xzy"))
                .get();

        ////////////////////////// close
        esStartStop.close(client);
    }

}
