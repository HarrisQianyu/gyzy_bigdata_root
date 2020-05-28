package com.cn.tz14.springcloud.es.controller;

import io.swagger.annotations.Api;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Set;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-21 21:56
 */
@Controller //控制器，转发器
@RequestMapping("/es")
@Api(value = "ES查询")
public class EsController {


    //传入经纬度，时间，索引名
    public Set<String> getLocus(){

        return null;
    }
}
