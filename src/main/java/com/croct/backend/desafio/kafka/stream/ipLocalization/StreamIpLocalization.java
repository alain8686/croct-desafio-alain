package com.croct.backend.desafio.kafka.stream.ipLocalization;

import java.time.Duration;
import java.util.Properties;

import com.croct.backend.desafio.dao.IPStackManager;
import com.croct.backend.desafio.dto.client.serializable.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KafkaStreams;

import com.croct.backend.desafio.dto.client.ClientIpDto;
import com.croct.backend.desafio.dao.IPStackManagerImp;

public class StreamIpLocalization {
    public static final String topicIn = "topic-in";
    public static final String topicOut = "topic-out";

    private Properties getKafkaConfig(){
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVER", "localhost:9092");

        return getKafkaConfig(bootstrapServers);
    }

    public Properties getKafkaConfig(String bootstrapServers){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "id-desafio-croft");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ClientIpSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, StreamIpTimeExtractor.class);

        return props;
    }

    public Topology create_stream_ip_handler(final String topip_in, final String topic_out,
                                             IPStackManager stackManager, long time_minutes){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ClientIpDto> clientIps = builder.stream(topip_in);
        TimeWindowedKStream<String, ClientIpDto> gropedStream = clientIps
                .groupBy((key, client)-> client.getClientId() + "_" + client.getIp())
                .windowedBy(TimeWindows.of(Duration.ofMinutes(time_minutes) ));
        KStream<Windowed<String>, ClientIpDto> client_resumed = gropedStream.aggregate(new Initializer<ClientIpDto>() {
            @Override
            public ClientIpDto apply() {
                return null;
            }
        }, new Aggregator<String,
                ClientIpDto, ClientIpDto>() {
            @Override
            public ClientIpDto apply(String s, ClientIpDto clientIpDto, ClientIpDto o) {
                if(o == null){
                    ClientIpDto located = stackManager.traceIp(clientIpDto);
                    if(located == null)
                        return clientIpDto;
                    else
                        return located;
                }
                return o;
            }
        }).toStream();

        client_resumed.to(topic_out, Produced.with(new WindowedString(), new ClientIpSerde()));
        return builder.build();
    }

    public void run(){
        try {
            IPStackManager stackManager = new IPStackManagerImp();
            KafkaStreams streams = new KafkaStreams(
                    this.create_stream_ip_handler(topicIn, topicOut, stackManager, 30), getKafkaConfig());
            streams.start();
        }
        catch (Exception ex){}
    }
}
