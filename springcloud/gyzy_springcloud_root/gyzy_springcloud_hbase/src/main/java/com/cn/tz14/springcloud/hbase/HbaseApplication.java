package com.cn.tz14.springcloud.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-20 22:26
 */
@SpringBootApplication
@EnableEurekaServer
public class HbaseApplication {
    public static void main(String[] args) {
        SpringApplication.run(HbaseApplication.class,args);
    }
}
