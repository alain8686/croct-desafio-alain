package com.croct.backend.desafio;

import com.croct.backend.desafio.dto.client.ClientIpDto;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.croct.backend.desafio.dao.IPStackManagerImp;

import static org.junit.Assert.*;

/**
 * Unit test for simple App.
 */
public class IPStackManagerTest {
    IPStackManagerImp ipStack;
    List<String> ips;

    @Before
    public void start() {
        ips = new ArrayList<String>(){{add("134.201.250.155");}};
        ipStack = new IPStackManagerImp();
    }

    @Test
    public void test_traceIp_http_service() {
        Random gerador = new Random();

        for(String ip: ips){
            ClientIpDto client = new ClientIpDto();
            client.setClientId(UUID.randomUUID().toString());
            client.setTimestamp(System.currentTimeMillis());
            client.setIp(ip);

            ipStack.traceIp(client);
            assertNotNull(client.getCity());
        }
    }
}
