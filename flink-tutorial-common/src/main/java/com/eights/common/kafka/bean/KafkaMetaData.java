package com.eights.common.kafka.bean;

import java.util.Objects;

public class KafkaMetaData {

    private long offset;

    private String topic;

    private int partition;

    public KafkaMetaData() {
    }

    public KafkaMetaData(long offset, String topic, int partition) {
        this.offset = offset;
        this.topic = topic;
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaMetaData that = (KafkaMetaData) o;
        return offset == that.offset &&
                partition == that.partition &&
                Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, topic, partition);
    }

    @Override
    public String toString() {
        return "KafkaMetaData{" +
                "offset=" + offset +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                '}';
    }
}
