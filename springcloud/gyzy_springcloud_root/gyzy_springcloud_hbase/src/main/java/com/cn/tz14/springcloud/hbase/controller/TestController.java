package com.cn.tz14.springcloud.hbase.controller;

import io.swagger.annotations.Api;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-21 20:04
 */
@Controller //控制器，转发器
@RequestMapping("/test")
@Api(value = "接口测试")
public class TestController {

    @ResponseBody
    @RequestMapping(value = "/helloWorld",method = {RequestMethod.GET,RequestMethod.POST})
    public String helloWorld(){

        //调用hbase查询接口
        System.out.println("hello world");
        return "hello world";
    }
}
