package com.jun.elasticsearch.api.update;

import com.jun.elasticsearch.entity.CustomAudience;
import com.jun.elasticsearch.entity.Targeting;
import com.jun.elasticsearch.global.MyElasticsearchClient;
import com.jun.util.JSONUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

public class UpdateApiTest {

    public void updateApiSync(String index, String type, String id, String jsonString) {
        try (final RestHighLevelClient client = MyElasticsearchClient.getInstance()){

            final UpdateRequest request = new UpdateRequest()
                    .index(index)
                    .type(type)
                    .id(id)
                    .timeout(TimeValue.timeValueMinutes(2))
                    .docAsUpsert(true)
                    .fetchSource(true)
                    .doc(jsonString, XContentType.JSON);

            final UpdateResponse response = client.update(request);

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

    public void updateApiAsyncTest(String index, String type, String id, String jsonString) {
        try (final RestHighLevelClient client = MyElasticsearchClient.getInstance()){

            final UpdateRequest request = new UpdateRequest()
                    .index(index)
                    .type(type)
                    .id(id)
                    .timeout(TimeValue.timeValueMinutes(2))
                    .docAsUpsert(true)
                    .fetchSource(true)
                    .doc(jsonString, XContentType.JSON);

            client.updateAsync(request, new ActionListener<UpdateResponse>() {
                @Override
                public void onResponse(UpdateResponse response) {
                    System.out.println("response：\n" +
                            "index：" + response.getIndex() + "\n" +
                            "type：" + response.getType() + "\n" +
                            "id：" + response.getId() + "\n" +
                            "version：" + response.getVersion());
                    System.out.println(request.toString());
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

    @Test
    public void testUpdate() {
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

        updateApiSync("test", "targeting", "b6AS1mQBRuvjKT8Aq6mn", JSONUtil.ObjectToJSONStringInSnakeCase(targeting));
    }

}
