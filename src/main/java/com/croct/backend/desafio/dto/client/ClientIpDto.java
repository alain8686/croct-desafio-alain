package com.croct.backend.desafio.dto.client;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ClientIpDto {
    private String clientId;
    private Long timestamp;
    private String ip;
    private Double latitude;
    private Double longitude;
    private String country;
    private String region;
    private String city;
}
