package com.cn.tz14.springcloud.hbase.controller;

import com.cn.tz14.springcloud.hbase.service.HbaseService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-21 20:15
 */
@Controller //控制器，转发器
@RequestMapping("/hbase")
@Api(value = "hbase查询")
public class HbaseController {

    @Autowired
    private HbaseService hbaseService;


    //两步查询获取所有关联关系

    //第一步。查询索引表，获取所有的主表主键

    @ApiOperation(value = "hbase索引表查询",notes = "查hbase索引表")
    @ApiImplicitParams({
            @ApiImplicitParam(name ="table",value = "索引表名，不带命名空间",required = true,dataType = "string"),
            @ApiImplicitParam(name ="rowkey",value = "hbase主键名",required = true,dataType = "string"),
    })
    @ResponseBody
    @RequestMapping(value = "/getRowkeys",method = {RequestMethod.GET,RequestMethod.POST})
    public Set<String> getRowkeys(@RequestParam(name="table") String table,
                                  @RequestParam(name="rowkey") String rowkey){
        //查询索引表，获取所有的主表主键
        return hbaseService.getRowkeys(table,rowkey,1000);
    }



    @ApiOperation(value = "一键搜索",notes = "一键搜索")
    @ApiImplicitParams({
            @ApiImplicitParam(name ="field",value = "数据类型=表名",required = true,dataType = "string"),
            @ApiImplicitParam(name ="fieldValue",value = "数据值 = rowkey",required = true,dataType = "string"),
    })
    //第二部，一键搜，输入：数据类型， 数据值   结果：所有关联关系
    @ResponseBody
    @RequestMapping(value = "/getRelation",method = {RequestMethod.GET,RequestMethod.POST})
    public Map<String,List<String>> getRelation(@RequestParam(name="field") String field,
                                                @RequestParam(name="fieldValue") String fieldValue){

        //查询索引表，获取所有的主表主键
        return hbaseService.getRelation(field,fieldValue);
    }








    @ResponseBody
    @RequestMapping(value = "/getMap",method = {RequestMethod.GET,RequestMethod.POST})
    public Map<String,String> getMap(@RequestParam(name="table") String table,
                                     @RequestParam(name="rowkey") String rowkey){
        System.out.println("=========方法getMap被调用==========");
        return hbaseService.getMap(table,rowkey);
    }

}
