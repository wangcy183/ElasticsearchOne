package com.dfjinxin.elasticsearch;

import com.dfjinxin.elasticsearch.config.ElasticConfig;
import org.elasticsearch.action.get.GetResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.net.UnknownHostException;

@SpringBootApplication
@RestController
public class ElasticsearchApplication {


    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchApplication.class, args);
    }


}
