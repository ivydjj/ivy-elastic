package com.ivy.transportclient;

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;

public class DeleteByQueryApi {
    public static void main(String[] args) throws IOException {
        EsStartStop esStartStop = new EsStartStop();
        TransportClient client = esStartStop.getClient();

        /////////////////////////// delete by query api
        // 先添加数据
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        IndexResponse indexResponse1 = client.prepareIndex("twitter", "tweet")
                .setSource(json, XContentType.JSON)
                .get();

        // 同步查询删除
        BulkByScrollResponse bulkByScrollResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                // query
                .filter(QueryBuilders.matchQuery("user", "kimchy"))
                // index
                .source("twitter")
                // execute the operation
                .get();
        // number of deleted documents
        long deleted = bulkByScrollResponse.getDeleted();
        System.out.println(deleted);

        // 异步查询删除，asynchronously
        // ???????????????? 有异常
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("user", "kimchy"))
                .source("twitter")
                // listener
                .execute(new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                            /*
                            org.elasticsearch.common.util.concurrent.EsRejectedExecutionException: rejected execution of org.elasticsearch.action.support.
                            ThreadedActionListener$2@223920ae on EsThreadPoolExecutor[listener, org.elasticsearch.common.util.concurrent.
                            EsThreadPoolExecutor@48dade3e[Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]]
                             */

                        // number of deleted documents
                        long deleted = response.getDeleted();
                        System.out.println(deleted);
                    }
                    @Override
                    public void onFailure(Exception e) {
                        // Handle the exception
                        e.printStackTrace();
                        System.out.println("Handle the exception when it is failed");
                    }
                });


        ////////////////////////// close
        esStartStop.close(client);
    }

}
