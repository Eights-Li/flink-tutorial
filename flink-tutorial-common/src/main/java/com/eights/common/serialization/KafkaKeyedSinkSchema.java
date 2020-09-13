package com.eights.common.serialization;

import com.eights.common.bean.KafkaMessage;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class KafkaKeyedSinkSchema implements KeyedSerializationSchema<KafkaMessage> {
    @Override
    public byte[] serializeKey(KafkaMessage element) {
        return element.getKey().getBytes();
    }

    @Override
    public byte[] serializeValue(KafkaMessage element) {
        return element.getValue().getBytes();
    }

    @Override
    public String getTargetTopic(KafkaMessage element) {
        return null;
    }
}
