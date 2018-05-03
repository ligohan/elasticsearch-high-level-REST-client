package com.jun.elasticsearch.entity;

import com.jun.elasticsearch.api.ElasticsearchApi;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.inject.Singleton;

public class MyElasticsearchClient {

    private static RestHighLevelClient client = null;

    public static RestHighLevelClient getInstance() {
        if (client == null) {
            synchronized (Singleton.class) {
                if (client == null) {
                    try {
                        client = new RestHighLevelClient(
                                RestClient.builder(
                                        new HttpHost(ElasticsearchApi.HOST_NAME, ElasticsearchApi.PORT, ElasticsearchApi.SCHEME)
                                )
                        );
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
        }
        return client;
    }

}
