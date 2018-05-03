package com.jun.elasticsearch.api.search;

import com.jun.elasticsearch.entity.MyElasticsearchClient;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SearchApi {

    public static Logger logger = Logger.getLogger(SearchApi.class);

    @Test
    public void searchApiSync(String[] indices, String[] types, String[] fieldNames, String keyWord) {

        try (RestHighLevelClient client = MyElasticsearchClient.getInstance()) {

            QueryBuilder matchQueryBuilder = QueryBuilders.multiMatchQuery(keyWord, fieldNames)
                    .fuzziness(Fuzziness.AUTO);

            SearchSourceBuilder builder = new SearchSourceBuilder()
                    .query(matchQueryBuilder)
                    .from(0)
                    .size(5)
                    .timeout(new TimeValue(60, TimeUnit.SECONDS));

            SearchRequest request = new SearchRequest(indices)
                    .types(types)
                    .source(builder);

            SearchResponse response = client.search(request);

            System.out.println("=== HTTP Request ===");
            System.out.println("status: " + response.status());
            System.out.println("took: " + response.getTook());
            System.out.println("timed_out: " + response.isTimedOut());

            System.out.println("\n=== Hits ===");
            SearchHits hits = response.getHits();
            System.out.println("total_hits: " + hits.getTotalHits());

            for (SearchHit hit : hits.getHits()) {
                System.out.println("\n=== Documents ===");
                System.out.println("index: " + hit.getIndex());
                System.out.println("type: " + hit.getType());
                System.out.println("id: " + hit.getId());
                System.out.println("source: " + hit.getSourceAsString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void searchApiAsync(String[] indices, String[] types, String[] fieldNames, String keyWord) {
        try (RestHighLevelClient client = MyElasticsearchClient.getInstance()) {

            QueryBuilder matchQueryBuilder = QueryBuilders.multiMatchQuery(keyWord, fieldNames)
                    .fuzziness(Fuzziness.AUTO);

            SearchSourceBuilder builder = new SearchSourceBuilder()
                    .query(matchQueryBuilder)
                    .from(0)
                    .size(5)
                    .timeout(new TimeValue(60, TimeUnit.SECONDS));

            SearchRequest request = new SearchRequest(indices)
                    .types(types)
                    .source(builder);

            client.searchAsync(request, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    System.out.println("=== HTTP Request ===");
                    System.out.println("status: " + searchResponse.status());
                    System.out.println("took: " + searchResponse.getTook());
                    System.out.println("timed_out: " + searchResponse.isTimedOut());

                    System.out.println("\n=== Hits ===");
                    SearchHits hits = searchResponse.getHits();
                    System.out.println("total_hits: " + hits.getTotalHits());

                    for (SearchHit hit : hits.getHits()) {
                        System.out.println("\n=== Documents ===");
                        System.out.println("index: " + hit.getIndex());
                        System.out.println("type: " + hit.getType());
                        System.out.println("id: " + hit.getId());
                        System.out.println("source: " + hit.getSourceAsString());
                    }
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

    /**
     * 　* @Description sort 排序
     *
     * @author lee jun
     * @date 2018/4/25 9:55
     * 　* @param
     * 　* @return
     * 　* @throws
     */
    @Test
    public void demo1() {

        QueryBuilder matchQueryBuilder = QueryBuilders
                .matchQuery("make", "honda");

        SortBuilder sortBuilder = SortBuilders.fieldSort("price").order(SortOrder.DESC);

        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(matchQueryBuilder)
                .sort(sortBuilder)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));

        excuteTestSearch(builder);
    }

    /**
     * 　* @Description 分页查询
     *
     * @author lee jun
     * @date 2018/4/25 9:58
     * 　* @param
     * 　* @return
     * 　* @throws
     */
    @Test
    public void demo2() {

        QueryBuilder matchQueryBuilder = QueryBuilders
                .matchAllQuery();

        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(matchQueryBuilder)
                .from(0)
                .size(1)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));
        excuteTestSearch(builder);
    }

    /**
     * 　* @Description range 查询
     *
     * @author lee jun
     * @date 2018/4/25 9:59
     * 　* @param
     * 　* @return
     * 　* @throws
     */
    @Test
    public void demo3() {

        QueryBuilder rangeQueryBuilder = QueryBuilders
                .rangeQuery("price").gte(30000);

        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(rangeQueryBuilder)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));

        excuteTestSearch(builder);
    }

    /**
     * 　* @Description term 查询
     *
     * @author lee jun
     * @date 2018/4/25 10:00
     * 　* @param
     * 　* @return
     * 　* @throws
     */
    @Test
    public void demo4() {

        QueryBuilder termQueryBuilder = QueryBuilders
                .termQuery("color", "red");

        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(termQueryBuilder)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));

        excuteTestSearch(builder);
    }

    /**
     * 　* @Description match all 查询
     *
     * @author lee jun
     * @date 2018/4/25 10:04
     * 　* @param
     * 　* @return
     * 　* @throws
     */
    @Test
    public void demo5() {

        QueryBuilder matchAllQueryBuilder = QueryBuilders
                .matchAllQuery();

        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(matchAllQueryBuilder)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));

        excuteTestSearch(builder);
    }

    //--------聚合-----------

    /**
     * 　* @Description terms 聚合
     *
     * @author lee jun
     * @date 2018/4/25 9:36
     * 　* @param
     * 　* @return
     * 　* @throws
     */
    @Test
    public void demo6() {

        AggregationBuilder aggregationBuilder = AggregationBuilders
                .terms("popular_colors")
                .field("color");

        SearchSourceBuilder builder = new SearchSourceBuilder()
                .size(0)
                .aggregation(aggregationBuilder)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));

        excuteTestAggregation(builder);
    }

    /**
     * 　* @Description 度量
     *
     * @author lee jun
     * @date 2018/4/25 9:35
     * 　* @param
     * 　* @return
     * 　* @throws
     */
    @Test
    public void demo7() {

        AggregationBuilder aggregationBuilder = AggregationBuilders
                .terms("colors")
                .field("color");

        AggregationBuilder AvgPriceAggregationBuilder = AggregationBuilders
                .avg("avg_price")
                .field("price");

        aggregationBuilder.subAggregation(AvgPriceAggregationBuilder);

        SearchSourceBuilder builder = new SearchSourceBuilder()
                .size(0)
                .aggregation(aggregationBuilder)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));

        excuteTestAggregation(builder);
    }

    /**
     * 　* @Description 嵌套桶
     *
     * @author lee jun
     * @date 2018/4/25 9:32
     * 　* @param
     * 　* @return
     * 　* @throws
     */
    @Test
    public void demo8() {

        AggregationBuilder aggregationBuilder = AggregationBuilders
                .terms("colors")
                .field("color");

        AggregationBuilder AvgPriceAggregationBuilder = AggregationBuilders
                .avg("avg_price")
                .field("price");

        AggregationBuilder makeAggregationBuilder = AggregationBuilders
                .terms("make")
                .field("make");

        aggregationBuilder.subAggregation(AvgPriceAggregationBuilder);
        aggregationBuilder.subAggregation(makeAggregationBuilder);

        SearchSourceBuilder builder = new SearchSourceBuilder()
                .size(0)
                .aggregation(aggregationBuilder)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));

        excuteTestAggregation(builder);
    }

    /**
     * 　* @Description histogram 柱状图
     *
     * @author lee jun
     * @date 2018/4/25 9:37
     * 　* @param
     * 　* @return
     * 　* @throws
     */
    @Test
    public void demo9() {

        AggregationBuilder priceAggregationBuilder = AggregationBuilders
                .histogram("price")
                .field("price")
                .interval(20000);

        AggregationBuilder revenueAggregationBuilder = AggregationBuilders
                .sum("revenue")
                .field("price");

        priceAggregationBuilder.subAggregation(revenueAggregationBuilder);

        SearchSourceBuilder builder = new SearchSourceBuilder()
                .size(0)
                .aggregation(priceAggregationBuilder)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));

        excuteTestAggregation(builder);
    }

    public void excuteTestSearch(SearchSourceBuilder builder) {
        try (RestHighLevelClient client = MyElasticsearchClient.getInstance()) {

            SearchRequest request = new SearchRequest("cars")
                    .types("transactions")
                    .source(builder);

            SearchResponse response = client.search(request);

            System.out.println("=== HTTP Request ===");
            System.out.println("status: " + response.status());
            System.out.println("took: " + response.getTook());
            System.out.println("timed_out: " + response.isTimedOut());

            System.out.println("\n=== Hits ===");
            SearchHits hits = response.getHits();
            System.out.println("total_hits: " + hits.getTotalHits());

            for (SearchHit hit : hits.getHits()) {
                System.out.println("\n=== Documents ===");
                System.out.println("index: " + hit.getIndex());
                System.out.println("type: " + hit.getType());
                System.out.println("id: " + hit.getId());
                System.out.println("source: " + hit.getSourceAsString());
            }
            System.out.println("\n=== Builder ===");
            System.out.println(builder);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void excuteTestAggregation(SearchSourceBuilder builder) {
        try (RestHighLevelClient client = MyElasticsearchClient.getInstance()) {

            SearchRequest request = new SearchRequest("cars")
                    .types("transactions")
                    .source(builder);

            SearchResponse response = client.search(request);

            System.out.println("=== HTTP Request ===");
            System.out.println("status: " + response.status());
            System.out.println("took: " + response.getTook());
            System.out.println("timed_out: " + response.isTimedOut());

            System.out.println("\n=== Response ===");
            System.out.println(response);

            System.out.println("\n=== Builder ===");
            System.out.println(builder);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        SearchApi searchApi = new SearchApi();
        //searchApi.searchApiAsync(new String[]{"ik-test"}, new String[]{"message"},new String[]{"content"},"坑爹");
        searchApi.demo8();
    }
}
