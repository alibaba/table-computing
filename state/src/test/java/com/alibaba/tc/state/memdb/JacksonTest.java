package com.alibaba.tc.state.memdb;

import com.facebook.airlift.json.JsonCodec;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.ArrayType;
import com.fasterxml.jackson.databind.type.MapType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JacksonTest {
    @Test
    public void test() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JavaType stringType = mapper.getTypeFactory().constructType(String.class);
        ArrayType arrayType = mapper.getTypeFactory().constructArrayType(String.class);
        MapType mapType = mapper.getTypeFactory().constructMapType(HashMap.class, stringType, arrayType);
        ArrayType arrayMapType = mapper.getTypeFactory().constructArrayType(mapType);

        Map[] maps = mapper.readValue("[{\"index1\": [\"process_time_format\", \"event_id\"]}]", arrayMapType);
        maps = mapper.readValue("[{\"index1\": [\"process_time_format\"]}, {\"index2\": [\"event_id\", \"threshold\", \"process_time_format\"]}, {\"index3\": [\"threshold\", \"process_time_format\"]}]", arrayMapType);
        return;
    }

    @Test
    public void testJsonCodec()
    {
        JsonCodec<List<Map<String, List<String>>>> jsonCodec = JsonCodec.listJsonCodec(JsonCodec.mapJsonCodec(String.class, JsonCodec.listJsonCodec(String.class)));
        List<Map<String, List<String>>> maps = jsonCodec.fromJson("[{\"index1\": [\"process_time_format\", \"event_id\"]}]");
        maps = jsonCodec.fromJson("[{\"index1\": [\"process_time_format\"]}, {\"index2\": [\"event_id\", \"threshold\", \"process_time_format\"]}, {\"index3\": [\"threshold\", \"process_time_format\"]}]");
        return;
    }
}
