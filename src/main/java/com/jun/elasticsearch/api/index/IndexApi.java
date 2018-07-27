package com.jun.elasticsearch.api.index;

import com.jun.elasticsearch.entity.CustomAudience;
import com.jun.elasticsearch.entity.Targeting;
import com.jun.elasticsearch.global.MyElasticsearchClient;
import com.jun.util.JSONUtil;
import lombok.extern.log4j.Log4j2;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

@Log4j2
public class IndexApi {

    public void indexApiSync(String index, String type, String id, String jsonString) {

        RestHighLevelClient client = MyElasticsearchClient.getInstance();

        final IndexRequest request = new IndexRequest()
                .index(index)
                .type(type)
                .id(id)
                .timeout(TimeValue.timeValueMinutes(2))
                .source(jsonString, XContentType.JSON);

        try {
            final IndexResponse response = client.index(request);
        } catch (IOException e) {
            e.printStackTrace();
        }


        //log.info("response：\n" +
        //        "index：" + response.getIndex() + "\n" +
        //        "type：" + response.getType() + "\n" +
        //        "id：" + response.getId() + "\n" +
        //        "version：" + response.getVersion());
        //
        //log.info(request.toString());

    }

    public void indexApiAsync(String index, String type, String id, Map<String, Object> map) {

        try (final RestHighLevelClient client = MyElasticsearchClient.getInstance()) {

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

    @Test
    public void testIndex() {

        CustomAudience customAudience = new CustomAudience();
        customAudience.setId("23842852893410737");
        customAudience.setName("类似受众 (EEA, 1%) - ATC-intimate-180d");

        Targeting targeting = new Targeting();
        List<String> customAudiences = new ArrayList<>();
        customAudiences.add(customAudience.getId());
        targeting.setCustomAudiences(Arrays.asList(customAudience));

        targeting.setDevicePlatforms(Arrays.asList("mobile", "desktop"));

        Map<String, List<String>> geoLocations = new HashMap<>(1);
        geoLocations.put("location_types", Arrays.asList("home", "recent"));
        geoLocations.put("cities", Arrays.asList("beijing"));
        geoLocations.put("countries", Arrays.asList("china"));
        targeting.setGeoLocations(geoLocations);

        targeting.setPublisherPlatforms(Arrays.asList("instagram", "messenger"));

        IndexApi indexApi = new IndexApi();
        indexApi.indexApiSync("test", "targeting", null, JSONUtil.ObjectToJSONStringInSnakeCase(targeting));

        log.info("{} 插入完成", JSONUtil.ObjectToJSONStringInSnakeCase(targeting));

    }

    @Test
    public void millionIndexTest() {
        CustomAudience customAudience = new CustomAudience();
        customAudience.setId("23842852893410737");
        customAudience.setName("类似受众 (EEA, 1%) - ATC-intimate-180d");

        Targeting targeting = new Targeting();
        List<String> customAudiences = new ArrayList<>();
        customAudiences.add(customAudience.getId());
        targeting.setCustomAudiences(Arrays.asList(customAudience));

        targeting.setDevicePlatforms(Arrays.asList("mobile", "desktop"));

        Map<String, List<String>> geoLocations = new HashMap<>(1);
        geoLocations.put("location_types", Arrays.asList("home", "recent"));
        geoLocations.put("cities", Arrays.asList("beijing"));
        geoLocations.put("countries", Arrays.asList("china"));
        targeting.setGeoLocations(geoLocations);

        targeting.setPublisherPlatforms(Arrays.asList("instagram", "messenger"));

        IndexApi indexApi = new IndexApi();

        long startTime = Instant.now().toEpochMilli();
        int i = 0;
        while (i < 10000) {
            if (geoLocations.get("location_types").equals(Arrays.asList("home", "recent"))) {
                geoLocations.put("location_types", Arrays.asList("business", "recent"));
            } else {
                geoLocations.put("location_types", Arrays.asList("home", "recent"));
            }
            indexApi.indexApiSync("test", "targeting", null, JSONUtil.ObjectToJSONStringInSnakeCase(targeting));
            i++;
            if (i % 10000 == 0) {
                log.info("{} 条数据插入完成", i);
            }
        }

        long endTime = Instant.now().toEpochMilli();

        log.info("批量插入完成，总耗时 {} ms", endTime - startTime);
    }

}
