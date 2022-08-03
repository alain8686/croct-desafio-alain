package com.croct.backend.desafio.dao;

import com.croct.backend.desafio.dto.client.ClientIpDto;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public interface IPStackManager {
    public ClientIpDto traceIp(ClientIpDto client);
}
