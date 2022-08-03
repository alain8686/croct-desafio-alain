package com.croct.backend.desafio.dao;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import com.croct.backend.desafio.dto.client.ClientIpDto;
import org.json.JSONObject;

public class IPStackManagerImp implements IPStackManager{
    private static HttpURLConnection conn;
    private String url;

    public IPStackManagerImp(){
        url = System.getenv().getOrDefault("IP_SERVICE_URL",
                "http://api.ipstack.com/%s?access_key=7c0b42bff59ace93b5e48b94eec2373a");
    }

    public ClientIpDto traceIp(ClientIpDto client) {
        String url = String.format(this.url, client.getIp());
        try {
            URL req = new URL(url);
            conn = (HttpURLConnection) req.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);

            int status = conn.getResponseCode();
            if(status == 200){
                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String line;
                StringBuilder responseContent = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    responseContent.append(line);
                }
                reader.close();

                JSONObject jsonObj = new JSONObject(responseContent.toString());
                client.setLatitude(jsonObj.getDouble("latitude"));
                client.setLongitude(jsonObj.getDouble("longitude"));
                client.setCountry(jsonObj.getString("country_name"));
                client.setRegion(jsonObj.getString("region_name"));
                client.setCity(jsonObj.getString("city"));

                return client;
            }
        } catch (Exception ex){}
        return null;
    }
}
