package com.eights.common.kafka.deserialization;




import com.eights.common.kafka.bean.KafkaMessage;
import com.eights.common.kafka.bean.KafkaMetaData;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * 解析kafka message中的key value metadata
 * @author Eights
 */
@PublicEvolving
public class KafkaMessageDeserializationSchema implements KafkaDeserializationSchema<KafkaMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageDeserializationSchema.class);

    private static final long serialVersionUID = 1509391548173891955L;

    private final boolean includeMetadata;

    public KafkaMessageDeserializationSchema(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    @Override
    public KafkaMessage deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

        KafkaMessage kafkaMessage = new KafkaMessage();

        try {

            if (record.key() != null && record.key().length > 0) {
                kafkaMessage.setKey(new String(record.key()));
            }
            if (record.value() != null) {
                kafkaMessage.setValue(new String(record.value()));
            }
            if (includeMetadata) {
                KafkaMetaData metaData = new KafkaMetaData(record.offset(), record.topic(), record.partition());
                kafkaMessage.setMetaData(metaData);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            LOG.error(String.format("KafkaMessageDeserializationSchema 出错：[%s] ======key: [%s] ========value: [%s]",
                    record.toString(), new String(record.key()), new String(record.value())));
        }

        return kafkaMessage;
    }

    @Override
    public boolean isEndOfStream(KafkaMessage nextElement) {
        return false;
    }

    @Override
    public TypeInformation<KafkaMessage> getProducedType() {
        return getForClass(KafkaMessage.class);
    }
}

