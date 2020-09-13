package com.eights.common.kafka.utilis;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class JSONUtils {


    private static final Logger LOG = LoggerFactory.getLogger(JSONUtils.class);

    /**
     * can use static singleton, inject: just make sure to reuse!
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JSONUtils() {
        //Feature that determines whether encountering of unknown properties, false means not analyzer unknown properties
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).setTimeZone(TimeZone.getDefault());
        // 对象的所有字段全部列入
        MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        // 取消默认转换timestamp形式
        MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        // 忽略空Bean转json的错误
        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        // 忽略在json字符串中存在，但是在java对象中不存在对应属性的情况，防止错误
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 支持单引号和没有引号
        MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    /**
     * json representation of object
     *
     * @param object object
     * @return object to json string
     */
    public static String toJson(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (Exception e) {
            LOG.error(String.format("object to json exception,:[%s]", e.getMessage()));
        }

        return null;
    }


    /**
     * This method deserializes the specified Json into an object of the specified class. It is not
     * suitable to use if the specified class is a generic type since it will not have the generic
     * type information because of the Type Erasure feature of Java. Therefore, this method should not
     * be used if the desired type is a generic type. Note that this method works fine if the any of
     * the fields of the specified object are generics, just the object itself should not be a
     * generic type.
     *
     * @param json  the string from which the object is to be deserialized
     * @param clazz the class of T
     * @param <T>   T
     * @return an object of type T from the string
     * classOfT
     */
    public static <T> T parseObject(String json, Class<T> clazz) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }

        try {
            return MAPPER.readValue(json, clazz);
        } catch (Exception e) {
            LOG.error(String.format("parse object exception:[%s]", e.getMessage()));
        }
        return null;
    }


    /**
     * json to list
     *
     * @param json  json string
     * @param clazz class
     * @param <T>   T
     * @return list
     */
    public static <T> List<T> toList(String json, Class<T> clazz) {
        if (StringUtils.isEmpty(json)) {
            return new ArrayList<>();
        }
        try {
            JavaType javaType = MAPPER.getTypeFactory().constructParametricType(List.class, clazz);
            return MAPPER.readValue(json, javaType);
        } catch (Exception e) {
            LOG.error(String.format("JSONArray.parseArray exception：[%s]", e.getMessage()));
        }

        return new ArrayList<>();
    }


    /**
     * check json object valid
     *
     * @param json json
     * @return true if valid
     */
    public static boolean checkJsonValid(String json) {

        if (StringUtils.isEmpty(json)) {
            return false;
        }

        try {
            MAPPER.readTree(json);
            return true;
        } catch (IOException e) {
            LOG.error(String.format("check json object valid exception:[%s]", e.getMessage()));
        }

        return false;
    }


    /**
     * Method for finding a JSON Object field with specified name in this
     * node or its child nodes, and returning value it has.
     * If no matching field is found in this node or its descendants, returns null.
     *
     * @param jsonNode  json node
     * @param fieldName Name of field to look for
     * @return Value of first matching node found, if any; null if none
     */
    public static String findValue(JsonNode jsonNode, String fieldName) {
        JsonNode node = jsonNode.findValue(fieldName);

        if (node == null) {
            return null;
        }

        return node.toString();
    }


    /**
     * json to map
     * <p>
     * {@link #toMap(String, Class, Class)}
     *
     * @param json json
     * @return json to map
     */
    public static Map<String, Object> toMap(String json) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }

        try {
            return MAPPER.readValue(json, new TypeReference<HashMap<String, Object>>() {
            });
        } catch (Exception e) {
            LOG.error(String.format("json to map exception:[%s]", e.getMessage()));
        }

        return null;
    }

    /**
     * json to map
     *
     * @param json   json
     * @param classK classK
     * @param classV classV
     * @param <K>    K
     * @param <V>    V
     * @return to map
     */
    public static <K, V> Map<K, V> toMap(String json, Class<K> classK, Class<V> classV) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }

        try {
            return MAPPER.readValue(json, new TypeReference<HashMap<K, V>>() {
            });
        } catch (Exception e) {
            LOG.error(String.format("json to map exception:[%s]", e.getMessage()));
        }

        return null;
    }

    /**
     * json serializer
     */
    public static class JsonDataSerializer extends JsonSerializer<String> {

        @Override
        public void serialize(String value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeRawValue(value);
        }

    }

    /**
     * json data deserializer
     */
    public static class JsonDataDeserializer extends JsonDeserializer<String> {

        @Override
        public String deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            return node.toString();
        }

    }
}
