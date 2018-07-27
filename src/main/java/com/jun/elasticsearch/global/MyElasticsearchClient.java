package com.jun.elasticsearch.global;

import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.inject.Singleton;

@Log4j2
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
                        log.error(e.getStackTrace());
                    }
                }
            }
        }
        return client;
    }

    public void destroy() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (final Exception e) {
            log.error("Error closing ElasticSearch client: ", e);
        }
    }

}
