package com.jun.elasticsearch.api.get;

import com.jun.elasticsearch.api.ElasticsearchApi;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class GetApiTest {

    @Test
    public void getApiSyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ElasticsearchApi.HOST_NAME, ElasticsearchApi.PORT, ElasticsearchApi.SCHEME)
                )
        )) {
            final GetRequest request = new GetRequest()
                    .index("index")
                    .type("logs")
                    .id("id");

            final GetResponse response = client.get(request);

            if (response.isExists()) {
                response.getSourceAsString();
               response.getSourceAsMap().get("key");
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void getApiAsyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ElasticsearchApi.HOST_NAME, ElasticsearchApi.PORT, ElasticsearchApi.SCHEME)
                )
        )) {
            final GetRequest request = new GetRequest()
                    .index("index")
                    .type("logs")
                    .id("id");

            client.getAsync(request, new ActionListener<GetResponse>() {
                @Override
                public void onResponse(final GetResponse response) {
                    response.getIndex();
                    response.getType();
                    response.getId();

                    if (response.isExists()) {
                        response.getSourceAsString();
                        response.getSourceAsMap().get("key");
                    }
                }

                @Override
                public void onFailure(final Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
