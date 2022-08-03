package com.croct.backend.desafio;

import com.croct.backend.desafio.dao.IPStackManager;
import com.croct.backend.desafio.dto.client.ClientIpDto;
import com.croct.backend.desafio.kafka.stream.ipLocalization.StreamIpLocalization;
import com.croct.backend.desafio.utils.TopicHelper;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


public class KafkaContainerWindowsTest {
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
    public void test_kafka_windows()throws Exception{
        IPStackManager stackManager = new IPStackManager() {
            @Override
            public ClientIpDto traceIp(ClientIpDto client) {
                Random r = new Random();

                client.setCity("Los Angeles");
                client.setCountry("United States");
                client.setRegion("California");
                client.setLatitude(r.nextDouble());
                client.setLongitude(r.nextDouble());
                return client;
            }
        };
        StreamIpLocalization stream = new StreamIpLocalization();

        String topicIn = StreamIpLocalization.topicIn + "_fake_http_service";
        String topicOut = StreamIpLocalization.topicOut + "_fake_http_service";

        Collection<NewTopic> topics_in = Collections.singletonList(new NewTopic(topicIn, 1, (short) 1));
        Collection<NewTopic> topics_out = Collections.singletonList(new NewTopic(topicOut, 1, (short) 1));
        adminClient.createTopics(topics_in).all().get(30, TimeUnit.SECONDS);
        adminClient.createTopics(topics_out).all().get(30, TimeUnit.SECONDS);

        configurate_stream_manager(kafka.getBootstrapServers(), stream, topicIn, topicOut, stackManager);

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
            ConsumerRecords<String, ClientIpDto> records = null;
            ClientIpDto first_client= null;
            while(records == null){
                records = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, ClientIpDto> record: records){
                    first_client = record.value();
                    break;
                }
            }

            producer.send(new ProducerRecord<>(StreamIpLocalization.topicIn, "testcontainers", client)).get();
            records = null;
            while(records == null){
                records = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, ClientIpDto> record: records){
                    Assert.assertEquals(first_client.getLatitude(), record.value().getLatitude());
                }
            }

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