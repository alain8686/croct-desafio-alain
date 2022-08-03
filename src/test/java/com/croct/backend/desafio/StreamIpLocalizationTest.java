package com.croct.backend.desafio;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import com.croct.backend.desafio.dao.IPStackManagerImp;
import com.croct.backend.desafio.dto.client.ClientIpDto;
import com.croct.backend.desafio.dto.client.serializable.ClientIpSerde;
import com.croct.backend.desafio.kafka.stream.ipLocalization.StreamIpLocalization;
import com.croct.backend.desafio.kafka.stream.ipLocalization.StreamIpTimeExtractor;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.croct.backend.desafio.dto.client.serializable.ClientIpSerializer;
import com.croct.backend.desafio.dto.client.serializable.ClientIpDeserializer;


public class StreamIpLocalizationTest {
    private final String topicIn = "topic-in";
    private final String topicOut = "topic-out";
    private TopologyTestDriver testDriver;
    private Properties properties;

    private Serdes.StringSerde stringSerde = new Serdes.StringSerde();
    private Serde<ClientIpDto> client_ip_serializer;

    private StreamIpLocalization ip_consumer;

    @Before
    public void start() {
        this.client_ip_serializer = Serdes.serdeFrom(new ClientIpSerializer(), new ClientIpDeserializer());

        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ClientIpSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, StreamIpTimeExtractor.class);

        ip_consumer = new StreamIpLocalization();
        Topology ip_manager_stream = ip_consumer.create_stream_ip_handler(this.topicIn, this.topicOut,
                new IPStackManagerImp(), 1);
        testDriver = new TopologyTestDriver(ip_manager_stream, properties);
    }

    @After
    public void tearDown() {
        Optional.ofNullable(testDriver).ifPresent(TopologyTestDriver::close);
        testDriver = null;
        properties = null;
    }

    @Test
    public void test_bad_ip(){
        TestInputTopic<String, ClientIpDto> inputTopic = testDriver.createInputTopic(topicIn,
                stringSerde.serializer(), this.client_ip_serializer.serializer());

        TestOutputTopic<String, ClientIpDto> outputTopic = testDriver.createOutputTopic(topicOut,
                stringSerde.deserializer(), this.client_ip_serializer.deserializer());

        ClientIpDto client = new ClientIpDto();
        client.setClientId(UUID.randomUUID().toString());
        client.setTimestamp(System.currentTimeMillis());
        client.setIp("-111.111.111.111");

        inputTopic.pipeInput(UUID.randomUUID().toString(), client);
        KeyValue<String, ClientIpDto> keyValue = outputTopic.readKeyValue();

        Assert.assertNull(keyValue.value.getLatitude());
    }

    @Test
    public void test_create_stream_ip_handler(){
        TestInputTopic<String, ClientIpDto> inputTopic = testDriver.createInputTopic(topicIn,
                stringSerde.serializer(), this.client_ip_serializer.serializer());
        TestOutputTopic<String, ClientIpDto> outputTopic = testDriver.createOutputTopic(topicOut,
                stringSerde.deserializer(), this.client_ip_serializer.deserializer());

        ClientIpDto client = new ClientIpDto();
        client.setClientId(UUID.randomUUID().toString());
        client.setTimestamp(System.currentTimeMillis());
        client.setIp("134.201.250.155");

        inputTopic.pipeInput(UUID.randomUUID().toString(), client);
        KeyValue<String, ClientIpDto> keyValue =  outputTopic.readKeyValue();

        Assert.assertTrue(keyValue.value.getIp().equalsIgnoreCase("134.201.250.155"));

        Assert.assertEquals(keyValue.value.getLatitude(), 34.0655517578125, 0.001);
        Assert.assertEquals(keyValue.value.getLongitude(), -118.24053955078125, 0.001);
        Assert.assertTrue(keyValue.value.getCountry().equals("United States") );
        Assert.assertTrue(keyValue.value.getRegion().equals("California"));
        Assert.assertTrue(keyValue.value.getCity().equals("Los Angeles"));
    }
}