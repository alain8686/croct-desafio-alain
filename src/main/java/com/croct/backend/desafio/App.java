package com.croct.backend.desafio;

import com.croct.backend.desafio.kafka.stream.ipLocalization.StreamIpLocalization;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {
        StreamIpLocalization consumer = new StreamIpLocalization();
        consumer.run();
    }
}
