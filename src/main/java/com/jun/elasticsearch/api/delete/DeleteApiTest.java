package com.jun.elasticsearch.api.delete;

import com.jun.elasticsearch.global.ElasticsearchApi;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class DeleteApiTest {

    @Test
    public void deleteApiSyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ElasticsearchApi.HOST_NAME, ElasticsearchApi.PORT, ElasticsearchApi.SCHEME)
                )
        )) {
            final DeleteRequest request = new DeleteRequest()
                    .index("index")
                    .type("logs")
                    .id("id")
                    .timeout(TimeValue.timeValueMinutes(2));

            final DeleteResponse response = client.delete(request);

            System.out.println(response.getResult());
            System.out.println(response.status());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void deleteApiAsyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ElasticsearchApi.HOST_NAME, ElasticsearchApi.PORT, ElasticsearchApi.SCHEME)
                )
        )) {
            final DeleteRequest request = new DeleteRequest()
                    .index("index")
                    .type("logs")
                    .id("id")
                    .timeout(TimeValue.timeValueMinutes(2));

            final CompletableFuture<String> future = new CompletableFuture<>();
            client.deleteAsync(request, new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse response) {

                    System.out.println(response.getResult());
                    System.out.println(response.status());

                    future.complete("ok");
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException(e);
                }
            });

        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
