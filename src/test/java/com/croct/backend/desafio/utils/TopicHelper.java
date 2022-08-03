package com.croct.backend.desafio.utils;

import com.croct.backend.desafio.dto.client.ClientIpDto;
import com.croct.backend.desafio.dto.client.serializable.ClientIpDeserializer;
import com.croct.backend.desafio.dto.client.serializable.ClientIpSerializer;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.rnorth.ducttape.unreliables.Unreliables;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class TopicHelper {
    public static KafkaProducer<String, ClientIpDto> configure_producer(String bootstrapServers){
        KafkaProducer<String, ClientIpDto> producer = new KafkaProducer<>(
                ImmutableMap.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        bootstrapServers,
                        ProducerConfig.CLIENT_ID_CONFIG,
                        UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new ClientIpSerializer()
        );
        return producer;
    }

    public static KafkaConsumer<String, ClientIpDto> configure_consumer(String bootstrapServers){
        KafkaConsumer<String, ClientIpDto> consumer = new KafkaConsumer<>(
                ImmutableMap.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG,
                        "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest"
                ),
                new StringDeserializer(),
                new ClientIpDeserializer()
        );
        return consumer;
    }

    public static void chack_test(KafkaConsumer<String, ClientIpDto> consumer, double lat, double lon, String country,
                            String region, String city){
        Unreliables.retryUntilTrue(60, TimeUnit.SECONDS,
                new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        ConsumerRecords<String, ClientIpDto> records = consumer.poll(Duration.ofMillis(100));
                        if(records.isEmpty())
                            return false;

                        for(ConsumerRecord<String, ClientIpDto> record: records){
                            Assert.assertEquals(record.value().getLatitude(), lat, 0.001);
                            Assert.assertEquals(record.value().getLongitude(), lon, 0.001);
                            Assert.assertTrue(record.value().getCountry().equals(country) );
                            Assert.assertTrue(record.value().getRegion().equals(region));
                            Assert.assertTrue(record.value().getCity().equals(city));
                        }
                        return true;
                    }
                }
        );
    }
}
