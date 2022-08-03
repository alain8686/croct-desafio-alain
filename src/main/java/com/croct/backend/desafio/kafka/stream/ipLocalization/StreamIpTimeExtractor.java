package com.croct.backend.desafio.kafka.stream.ipLocalization;


import com.croct.backend.desafio.dto.client.ClientIpDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;


public class StreamIpTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
        long timestamp = -1;
        final ClientIpDto client = (ClientIpDto) record.value();
        if (client != null) {
            timestamp = client.getTimestamp();
        }
        if (timestamp < 0) {
            if (previousTimestamp >= 0) {
                return previousTimestamp;
            } else {
                return System.currentTimeMillis();
            }
        }
        return timestamp;
    }

}
