package com.jun.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;

public class JSONUtil {

    public static SerializeConfig config = new SerializeConfig();

    static {
        // 生产环境中，config要做singleton处理，要不然会存在性能问题
        config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
    }

    public static String ObjectToJSONStringInSnakeCase(Object source) {
        String result = JSON.toJSONString(source, config);
        return result;
    }
}
