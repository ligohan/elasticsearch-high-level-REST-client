package com.jun.elasticsearch.api.bulk;

import com.jun.elasticsearch.global.ElasticsearchApi;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class BulkApiTest {

    @Test
    public void bulkApiTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ElasticsearchApi.HOST_NAME, ElasticsearchApi.PORT, ElasticsearchApi.SCHEME)
                )
        )) {
            final BulkRequest request = new BulkRequest()
                    .timeout(TimeValue.timeValueMinutes(2));
            request.add(new IndexRequest()
                    .index("index")
                    .type("logs")
                    .id("id")
                    .source("user", "hainet")
            );
            request.add(new DeleteRequest()
                    .index("index")
                    .type("logs")
                    .id("id")
            );

            final BulkResponse bulkResponse = client.bulk(request);

            bulkResponse.forEach(bulkItemResponse -> {
                final DocWriteResponse response = bulkItemResponse.getResponse();

                System.out.println("response：\n" +
                        "index：" + response.getIndex() + "\n" +
                        "type：" + response.getType() + "\n" +
                        "id：" + response.getId() + "\n" +
                        "version：" + response.getVersion());

                if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX) {
                    final IndexResponse indexResponse = (IndexResponse) response;
                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                    final DeleteResponse deleteResponse = (DeleteResponse) response;
                }
            });
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void bulkApiAsyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ElasticsearchApi.HOST_NAME, ElasticsearchApi.PORT, ElasticsearchApi.SCHEME)
                )
        )) {
            final BulkRequest request = new BulkRequest();
            request.add(new IndexRequest("index", "logs", "id").source("user", "hainet"));

            client.bulkAsync(request, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    // do something
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
