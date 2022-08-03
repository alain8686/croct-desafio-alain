package com.croct.backend.desafio.dto.client.serializable;

import com.croct.backend.desafio.dto.client.ClientIpDto;
import org.apache.kafka.common.serialization.Serdes;

public class ClientIpSerde extends Serdes.WrapperSerde<ClientIpDto>{
    public ClientIpSerde(){
        super(new ClientIpSerializer(), new ClientIpDeserializer());
    }
}
