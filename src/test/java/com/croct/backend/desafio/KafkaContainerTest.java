package com.croct.backend.desafio;

import com.croct.backend.desafio.dao.IPStackManager;
import com.croct.backend.desafio.dao.IPStackManagerImp;
import com.croct.backend.desafio.kafka.stream.ipLocalization.StreamIpLocalization;
import com.croct.backend.desafio.dto.client.ClientIpDto;

import com.croct.backend.desafio.utils.TopicHelper;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


public class KafkaContainerTest {
    KafkaContainer kafka;
    AdminClient adminClient;
    Runnable r;

    @Before
    public void start() throws Exception{
        kafka = new KafkaContainer();
        kafka.start();

        adminClient = AdminClient.create(
                ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
        );
    }

    @Test
    public void test_kafka_simple_request() throws Exception {
        StreamIpLocalization stream = new StreamIpLocalization();

        String topicIn = StreamIpLocalization.topicIn + "_simple_example";
        String topicOut = StreamIpLocalization.topicOut + "_simple_example";

        Collection<NewTopic> topics_in = Collections.singletonList(new NewTopic(topicIn, 1, (short) 1));
        Collection<NewTopic> topics_out = Collections.singletonList(new NewTopic(topicOut, 1, (short) 1));
        adminClient.createTopics(topics_in).all().get(30, TimeUnit.SECONDS);
        adminClient.createTopics(topics_out).all().get(30, TimeUnit.SECONDS);

        configurate_stream_manager(kafka.getBootstrapServers(), stream, StreamIpLocalization.topicIn,
                StreamIpLocalization.topicOut, new IPStackManagerImp());

        String bootstrapServers = kafka.getBootstrapServers();
        try (
                KafkaProducer<String, ClientIpDto> producer = TopicHelper.configure_producer(bootstrapServers);
                KafkaConsumer<String, ClientIpDto> consumer = TopicHelper.configure_consumer(bootstrapServers);
        ) {
            consumer.subscribe(Collections.singletonList(StreamIpLocalization.topicOut));

            ClientIpDto client = new ClientIpDto();
            client.setClientId(UUID.randomUUID().toString());
            client.setTimestamp(System.currentTimeMillis());
            client.setIp("134.201.250.155");

            producer.send(new ProducerRecord<>(StreamIpLocalization.topicIn, "testcontainers", client)).get();
            TopicHelper.chack_test(consumer, 34.0655517578125, -118.24053955078125, "United States",
                    "California", "Los Angeles");

            consumer.unsubscribe();
        }
    }

    private void configurate_stream_manager(String bootstrapServers, StreamIpLocalization stream, String topicIn,
                                            String topicOut, IPStackManager stackManager){
        if(r == null) {
            r = new Runnable() {
                @Override
                public void run() {
                    KafkaStreams streams = new KafkaStreams(
                            stream.create_stream_ip_handler(topicIn, topicOut, stackManager, 1),
                            stream.getKafkaConfig(bootstrapServers));
                    streams.start();
                }
            };
            r.run();
        }
    }


}