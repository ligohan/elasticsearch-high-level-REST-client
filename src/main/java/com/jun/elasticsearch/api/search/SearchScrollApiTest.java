package com.jun.elasticsearch.api.search;

import com.jun.elasticsearch.entity.MyElasticsearchClient;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class SearchScrollApiTest {

    /**
     * 　* @Description scroll 遍历
     *
     * @author lee jun
     * @date 2018/4/25 10:10
     * 　* @param
     * 　* @return
     * 　* @throws
     *
     */
    @Test
    public void demo10() {

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(3L));

        int time = 1;

        try (RestHighLevelClient client = MyElasticsearchClient.getInstance()) {

            SearchSourceBuilder builder1 = new SearchSourceBuilder()
                    .size(3)
                    .timeout(new TimeValue(60, TimeUnit.SECONDS));

            SearchRequest request1 = new SearchRequest("cars")
                    .types("transactions")
                    .source(builder1)
                    .scroll(scroll);

            SearchResponse searchResponse = client.search(request1);
            String scrollId = searchResponse.getScrollId();
            SearchHits hits = searchResponse.getHits();

            System.out.println("=== ScrollId " + time + " ===");
            System.out.println(scrollId);

            System.out.println("=== Hits " + time + " ===");
            System.out.println("hits_size: " + hits.getHits().length + "\n");

            time++;

            while (hits != null && hits.getHits().length > 0) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = client.searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                hits = searchResponse.getHits();

                System.out.println("=== ScrollId " + time + " ===");
                System.out.println(scrollId);

                System.out.println("=== Hits " + time + " ===");
                System.out.println("hits_size: " + hits.getHits().length + "\n");

                time++;
            }


            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
            System.out.println("\n=== Clear ScrollId ===");
            System.out.println("clear_success：" + clearScrollResponse.isSucceeded());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     　* @Description search_after 遍历
     * @author lee jun
     * @date 2018/4/25 11:50
    　* @param
    　* @return
    　* @throws
    　*/
    @Test
    public void demo11() {

        int time = 1;
        Object[] searchAfter = null;

        QueryBuilder matchQueryBuilder = QueryBuilders.matchAllQuery();
        SortBuilder sortPriceBuilder = SortBuilders.fieldSort("price").order(SortOrder.DESC);
        SortBuilder sortIdBuilder = SortBuilders.fieldSort("_id").order(SortOrder.DESC);

        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(matchQueryBuilder)
                .size(2)
                .sort(sortIdBuilder)
                .sort(sortPriceBuilder)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));

        try (RestHighLevelClient client = MyElasticsearchClient.getInstance()) {

            SearchRequest request = new SearchRequest("cars")
                    .types("transactions")
                    .source(builder);
            SearchResponse response = client.search(request);
            SearchHits hits = response.getHits();

            System.out.println("=== Hits " + time + " ===");
            System.out.println("hits_size: " + hits.getHits().length + "");

            for (SearchHit hit : hits.getHits()) {
                System.out.println("=== Documents===");
                System.out.println("id: " + hit.getId());
                for (int i = 0; i < hit.getSortValues().length; i ++) {
                    System.out.println("sort" + i + "：" +  hit.getSortValues()[i].toString());
                }
                searchAfter = hit.getSortValues();
            }

            time++;

            while (hits != null && hits.getHits().length > 0) {
                builder.searchAfter(searchAfter);
                response = client.search(request);
                hits = response.getHits();

                System.out.println("\n=== Hits " + time + " ===");
                System.out.println("hits_size: " + hits.getHits().length);

                for (SearchHit hit : hits.getHits()) {
                    System.out.println("=== Documents===");
                    System.out.println("id: " + hit.getId());
                    for (int i = 0; i < hit.getSortValues().length; i ++) {
                        System.out.println("sort" + i + "：" +  hit.getSortValues()[i].toString());
                    }
                    searchAfter = hit.getSortValues();
                }
                time++;
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
