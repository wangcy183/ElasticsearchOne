package com.dfjinxin.elasticsearch.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
public class ElasticConfig {

    @Bean
    public TransportClient transportClient() throws UnknownHostException {

        TransportAddress node = new TransportAddress(
                InetAddress.getByName("localhost"),9300
        );
        Settings settings=Settings.builder()
                .put("cluster.name","wcy") //集群名称
                .put("client.transport.sniff",true)//目的是为了可以找到集群，嗅探机制开启
                .build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(node);
        return client;
    }

}
