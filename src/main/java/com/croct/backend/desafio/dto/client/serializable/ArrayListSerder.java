package com.croct.backend.desafio.dto.client.serializable;

import com.croct.backend.desafio.dto.client.ClientIpDto;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ArrayListSerder extends Serdes.WrapperSerde<List<ClientIpDto>> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ArrayListSerder(){
        super(new Serializer<List<ClientIpDto>>() {
            @Override
            public byte[] serialize(String s, List<ClientIpDto> clientIpDtos){
                try{
                    return new ObjectMapper().writeValueAsBytes(new WrapperList(clientIpDtos));
                }catch (Exception ex){

                }
                return null;
            }
        }, new Deserializer<List<ClientIpDto>>() {
            @Override
            public List<ClientIpDto> deserialize(String s, byte[] bytes){
                try{
                    WrapperList wrapper = new ObjectMapper().readValue(new String(bytes, "UTF-8"), WrapperList.class);
                    return wrapper.subList(0, wrapper.size());
                }catch (Exception ex){}
                return null;
            }
        });
    }
}

class WrapperList extends ArrayList<ClientIpDto>{
     public WrapperList(List<ClientIpDto> a){
         super(a);
     }
}
