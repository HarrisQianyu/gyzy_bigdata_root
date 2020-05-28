package com.cn.tz14.springcloud.hbase.service;

import org.apache.hadoop.hbase.client.Get;
import org.springframework.stereotype.Service;
import gzzy_bigdata_root.hbase.entity.HBaseCell;
import gzzy_bigdata_root.hbase.entity.HBaseRow;
import gzzy_bigdata_root.hbase.extractor.MapRowExtrator;
import gzzy_bigdata_root.hbase.extractor.MultiVersionRowExtrator;
import gzzy_bigdata_root.hbase.extractor.SingleColumnMultiVersionRowExtrator;
import gzzy_bigdata_root.hbase.search.HBaseSearchService;
import gzzy_bigdata_root.hbase.search.HBaseSearchServiceImpl;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-21 20:36
 */
@Service
public class HbaseService {


    @Resource
    private HbaseService hbaseService;

    /**
     * 查询索引表，获取所有的主表主键
     * @param table
     * @param rowkey
     * @return
     */
    public Set<String> getRowkeys(String table,String rowkey,Integer versionNum){

        Set<String> rowKeys = null;
        HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();

        String tableName = "test:" + table;

        SingleColumnMultiVersionRowExtrator singleColumnMultiVersionRowExtrator=new SingleColumnMultiVersionRowExtrator("cf".getBytes(),"phone_mac".getBytes(),new HashSet<>());
        //需要设置一下版本号
        try {
            Get get = new Get(rowkey.getBytes());
            get.setMaxVersions(versionNum);
            rowKeys = hBaseSearchService.search(tableName, get, singleColumnMultiVersionRowExtrator);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rowKeys;
    }

    /**
     *
     * @param field
     * @param fieldValue
     * @return
     */
    public Map<String,List<String>> getRelation(String field,String fieldValue){
          // 获取所有的主键
        Map<String,List<String>> mapList = new HashMap<>();

        //所有的主表MACS
        Set<String> macs = hbaseService.getRowkeys(field, fieldValue, 1000);
        // 遍历主键，查询所有的详细信息
        HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();
        List<Get> list = new ArrayList<>();
        macs.forEach(index_mac->{
             //因为会有多个MAC，转为批量差
            try {
                Get get = new Get(index_mac.getBytes());
                get.setMaxVersions(1000);
                list.add(get);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        try {
            //查主表
            String tableName = "test:relation";
            List<HBaseRow> search = hBaseSearchService.search(tableName, list, new MultiVersionRowExtrator());

            search.forEach(hbaseRow->{
                //获取到每个rowkey的所有信息，其实就是cell
                Map<String, Collection<HBaseCell>> cellMap = hbaseRow.getCell();
                cellMap.forEach((k,v)->{
                //解析的结果需要存放到最终的集合里面去
                    if(mapList.containsKey(k)&&!mapList.get(k).isEmpty()){
                        //如果maplist里面已经包含了rowkey,那么说明之前有这个rowkey的数据放进去过
                        // 这时我们需要从mapList里面获取原来的数据，然后把新循环遍历的数据存放进去
                        List<String> listValue = mapList.get(k);

                        Iterator<HBaseCell> iterator = v.iterator();
                        while (iterator.hasNext()){
                            listValue.add(iterator.next().toString());
                        }
                        //最后重新放回 maplist
                        mapList.put(k,listValue);
                    }else{
                        //如果maplist里面步包含rowkey,说明第一次放入，直接放入
                        List<String> listValue = new ArrayList<>();
                        v.forEach(x->{
                            listValue.add(x.toString());
                        });
                        mapList.put(k,listValue);
                    }
                });

            });

        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapList;
    }


    /**
     *  HBASE根据表名，主键进行查询
     * @param table
     * @param rowkey
     * @return
     */
    public Map<String,String> getMap(String table,String rowkey){
        Map<String, String> search = null;
        try {
            Get get = new Get(rowkey.getBytes());
            String space_table= "test:"+table;
            HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();
            search = hBaseSearchService.search(space_table, get, new MapRowExtrator());

        } catch (IOException e) {
            e.printStackTrace();
        }
        return search;
    }
}
