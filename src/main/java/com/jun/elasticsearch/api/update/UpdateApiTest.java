package com.jun.elasticsearch.api.update;

import com.jun.elasticsearch.api.ElasticsearchApi;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class UpdateApiTest {

    @Test
    public void updateApiSyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ElasticsearchApi.HOST_NAME, ElasticsearchApi.PORT, ElasticsearchApi.SCHEME)
                )
        )) {
            // Map
            final HashMap<String, Object> map = new HashMap<>();
            map.put("message", "updated");

            // XContentBuilder
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("message", "updated");
            }
            builder.endObject();

            final UpdateRequest request = new UpdateRequest()
                    .index("index")
                    .type("logs")
                    .id("id")
                    .timeout(TimeValue.timeValueMinutes(2))
                    .docAsUpsert(true)
                    .fetchSource(true)
                    // Map
                    .doc(map);
                    // XContentBuilder
                    // .doc(builder);
                    // Object key-pairs
                    // .doc("message", "updated");

            final UpdateResponse response = client.update(request);

            response.getIndex();
            response.getType();
            response.getId();

            System.out.println(response.getResult());
            System.out.println(response.status());

            final GetResult result = response.getGetResult();
            if (result.isExists()) {
                result.sourceAsString();
                result.sourceAsMap().get("message");
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void updateApiAsyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ElasticsearchApi.HOST_NAME, ElasticsearchApi.PORT, ElasticsearchApi.SCHEME)
                )
        )) {
            // Map
            final HashMap<String, Object> map = new HashMap<>();
            map.put("message", "updated");

            // XContentBuilder
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("message", "updated");
            }
            builder.endObject();

            final UpdateRequest request = new UpdateRequest()
                    .index("index")
                    .type("logs")
                    .id("id")
                    .timeout(TimeValue.timeValueMinutes(2))
                    .docAsUpsert(true)
                    // Map
                    .doc(map);
            // XContentBuilder
            // .doc(builder);
            // Object key-pairs
            // .doc("message", "updated");

            final CompletableFuture<String> future = new CompletableFuture<>();
            client.updateAsync(request, new ActionListener<UpdateResponse>() {
                @Override
                public void onResponse(UpdateResponse response) {
                    response.getIndex();
                    response.getType();
                    response.getId();

                    System.out.println(response.getResult());
                    System.out.println(response.status());

                    final GetResult result = response.getGetResult();
                    if (result.isExists()) {
                        result.sourceAsString();
                        result.sourceAsMap().get("message");
                    }

                    future.complete("ok");
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException(e);
                }
            });

            future.get();
        } catch (final IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
