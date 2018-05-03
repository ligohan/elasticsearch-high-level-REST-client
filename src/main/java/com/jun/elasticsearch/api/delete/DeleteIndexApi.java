package com.jun.elasticsearch.api.delete;

import com.jun.elasticsearch.api.ElasticsearchApi;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

public class DeleteIndexApi {

    public void deleteIndexApiSync(String index) {
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ElasticsearchApi.HOST_NAME, ElasticsearchApi.PORT, ElasticsearchApi.SCHEME)
                )
        )) {
            DeleteIndexRequest request = new DeleteIndexRequest (index)
                    .timeout(TimeValue.timeValueMinutes(2));

            DeleteIndexResponse response = client.indices().delete(request);
            System.out.println("response：\n" +
                    "acknowledged ：" + response.isAcknowledged());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteIndexApiAsync(String index) {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ElasticsearchApi.HOST_NAME, ElasticsearchApi.PORT, ElasticsearchApi.SCHEME)
                )
        )) {
            final DeleteIndexRequest  request = new DeleteIndexRequest (index)
                    .timeout(TimeValue.timeValueMinutes(2));

            client.indices().deleteAsync(request, new ActionListener<DeleteIndexResponse>() {
                @Override
                public void onResponse(final DeleteIndexResponse response) {

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

    public static void main(String[] args) {
        DeleteIndexApi deleteIndexApi = new DeleteIndexApi();
        deleteIndexApi.deleteIndexApiSync("test");
    }
}
