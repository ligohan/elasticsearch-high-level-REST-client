package com.jun.elasticsearch.entity;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class Targeting {

    private List<CustomAudience> customAudiences;
    private List<String> devicePlatforms;
    private Map<String, List<String>> geoLocations;
    private List<String> publisherPlatforms;
}
