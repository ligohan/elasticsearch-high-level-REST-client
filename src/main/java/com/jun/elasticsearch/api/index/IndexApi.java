package com.jun.elasticsearch.api.index;

import com.jun.elasticsearch.entity.MyElasticsearchClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexApi {

    @Test
    public void indexApiSync(String index, String type, String id, Map<String, Object> map) {

        try (final RestHighLevelClient client = MyElasticsearchClient.getInstance()){

            final IndexRequest request = new IndexRequest()
                    .index(index)
                    .type(type)
                    .id(id)
                    .timeout(TimeValue.timeValueMinutes(2))
                    .source(map);

            final IndexResponse response = client.index(request);

            System.out.println("response：\n" +
                    "index：" + response.getIndex() + "\n" +
                    "type：" + response.getType() + "\n" +
                    "id：" + response.getId() + "\n" +
                    "version：" + response.getVersion());


            System.out.println(request.toString());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void indexApiAsync(String index, String type, String id, Map<String, Object> map) {

        try (final RestHighLevelClient client = MyElasticsearchClient.getInstance()){

            final IndexRequest request = new IndexRequest()
                    .index(index)
                    .type(type)
                    .id(id)
                    .timeout(TimeValue.timeValueMinutes(2))
                    .source(map);

            client.indexAsync(request, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(final IndexResponse response) {
                    System.out.println("response：\n" +
                            "index：" + response.getIndex() + "\n" +
                            "type：" + response.getType() + "\n" +
                            "id：" + response.getId() + "\n" +
                            "version：" + response.getVersion());
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

    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> map = new HashMap<>();
        map.put("content", "ElasticSearch是一个基于Lucene的搜索服务器。它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。");

        IndexApi indexApi = new IndexApi();
        indexApi.indexApiSync("ik", "message", "1", map);

    }
}
