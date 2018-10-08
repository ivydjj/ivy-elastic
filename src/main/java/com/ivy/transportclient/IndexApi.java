package com.ivy.transportclient;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;

public class IndexApi {
    public static void main(String[] args) throws IOException {
        EsStartStop esStartStop = new EsStartStop();
        TransportClient client = esStartStop.getClient();

        /////////////////////////// index api

        // the first method: add index
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject();

        IndexResponse indexResponse = client.prepareIndex("twitter", "tweet", "1")
                .setSource(builder)
                .get();

//            // the second method: add index
//            String json = "{" +
//                    "\"user\":\"kimchy\"," +
//                    "\"postDate\":\"2013-01-30\"," +
//                    "\"message\":\"trying out Elasticsearch\"" +
//                    "}";
//
//            IndexResponse response1 = client.prepareIndex("twitter", "tweet")
//                    .setSource(json, XContentType.JSON)
//                    .get();

        // IndexResponse object will give you a report
        // Index name
        String _index = indexResponse.getIndex();
        System.out.println(_index);
        // Type name
        String _type = indexResponse.getType();
        System.out.println(_type);
        // Document ID (generated or not)
        String _id = indexResponse.getId();
        System.out.println(_id);
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = indexResponse.getVersion();
        System.out.println(_version);
        // status has stored current instance statement.
        RestStatus status = indexResponse.status();
        System.out.println(status);

        ////////////////////////// close
        esStartStop.close(client);
    }

}
