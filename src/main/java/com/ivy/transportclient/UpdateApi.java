package com.ivy.transportclient;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/*
Update by script
Update by merging documents
Upsert
 */
public class UpdateApi {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        EsStartStop esStartStop = new EsStartStop();
        TransportClient client = esStartStop.getClient();

        /////////////////////////// update api
        // 先添加数据
        IndexResponse indexResponse = client.prepareIndex("twitter", "tweet", "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject())
                .get();

        GetResponse getResponse = client.prepareGet("twitter", "tweet", "1").get();
        System.out.println(getResponse.getSourceAsString());

        ///////// UpdateRequest
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("twitter");
        updateRequest.type("tweet");
        updateRequest.id("1");
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field("user", "updateRequest_kimchy")
                .endObject());
        client.update(updateRequest).get();

        getResponse = client.prepareGet("twitter", "tweet", "1").get();
        System.out.println(getResponse.getSourceAsString());

        ///////// prepareUpdate
        client.prepareUpdate("twitter", "tweet", "1")
                .setDoc(jsonBuilder()
                        .startObject()
                        .field("user", "prepareUpdate_kimchy")
                        .endObject())
                .get();

        getResponse = client.prepareGet("twitter", "tweet", "1").get();
        System.out.println(getResponse.getSourceAsString());

        ///////// upsert
        // 数据不存在时，先添加后修改；存在时，直接修改

        // 不存在的数据
        IndexRequest indexRequest = new IndexRequest("index", "type", "1")
                .source(jsonBuilder()
                        .startObject()
                        .field("name", "Joe Smith")
                        .field("gender", "Smith_male")
                        .endObject());

        // If the document does not exist, the content of the upsert element will be used to index the fresh doc
        updateRequest = new UpdateRequest("index", "type", "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject())
                .upsert(indexRequest);
        client.update(updateRequest).get();

        getResponse = client.prepareGet("index", "type", "1").get();
        System.out.println(getResponse.getSourceAsString());

        indexRequest = new IndexRequest("index", "type", "1")
                .source(jsonBuilder()
                        .startObject()
                        .field("name", "Joe Dalton")
                        .field("gender", "male")
                        .endObject());

        updateRequest = new UpdateRequest("index", "type", "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "Dalton_male")
                        .endObject())
                .upsert(indexRequest);
        client.update(updateRequest).get();

        getResponse = client.prepareGet("index", "type", "1").get();
        System.out.println(getResponse.getSourceAsString());

        ////////////////////////// close
        esStartStop.close(client);
    }

}
