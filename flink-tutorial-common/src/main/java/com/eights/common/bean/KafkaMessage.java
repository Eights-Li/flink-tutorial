package com.eights.common.bean;

import java.util.Objects;

public class KafkaMessage {

    private String key;

    private String value;

    private KafkaMetaData metaData;

    public KafkaMessage() {
    }

    public KafkaMessage(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public KafkaMessage(String key, String value, KafkaMetaData metaData) {
        this.key = key;
        this.value = value;
        this.metaData = metaData;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public KafkaMetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(KafkaMetaData metaData) {
        this.metaData = metaData;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaMessage that = (KafkaMessage) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(value, that.value) &&
                Objects.equals(metaData, that.metaData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, metaData);
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", metaData=" + metaData +
                '}';
    }
}
