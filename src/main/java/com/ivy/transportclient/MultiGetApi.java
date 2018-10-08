package com.ivy.transportclient;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class MultiGetApi {
    public static void main(String[] args) throws IOException {
        EsStartStop esStartStop = new EsStartStop();
        TransportClient client = esStartStop.getClient();

        /////////////////////////// get api
        // 先添加数据
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject();

        IndexResponse indexResponse = client.prepareIndex("twitter", "tweet", "1")
                .setSource(builder)
                .get();

        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                // get by a single id
                .add("twitter", "tweet", "1")
                // or by a list of ids for the same index / type
                .add("twitter", "tweet", "2", "3", "4")
                // you can also get from another index
                .add("another", "type", "foo")
                .get();

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response != null && response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println(json);
            }
        }

        ////////////////////////// close
        esStartStop.close(client);
    }

}
