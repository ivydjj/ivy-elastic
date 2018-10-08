package com.ivy.transportclient;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class DeleteApi {
    public static void main(String[] args) throws IOException {
        EsStartStop esStartStop = new EsStartStop();
        TransportClient client = esStartStop.getClient();

        /////////////////////////// delete api
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

        GetResponse getResponse = client.prepareGet("twitter", "tweet", "1").get();
        System.out.println(getResponse.getSourceAsString());

        DeleteResponse deleteResponse = client.prepareDelete("twitter", "tweet", "1").get();
        // 是否删除成功
        getResponse = client.prepareGet("twitter", "tweet", "1").get();
        System.out.println(getResponse.getSourceAsString());

        ////////////////////////// close
        esStartStop.close(client);
    }

}
