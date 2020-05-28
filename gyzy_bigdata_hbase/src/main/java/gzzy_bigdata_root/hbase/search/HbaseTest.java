package gzzy_bigdata_root.hbase.search;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import gzzy_bigdata_root.hbase.config.HBaseTableUtil;
import gzzy_bigdata_root.hbase.extractor.MapRowExtrator;
import gzzy_bigdata_root.hbase.insert.HBaseInsertHelper;
import gzzy_bigdata_root.hbase.spilt.SpiltRegionUtil;

import java.util.Map;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-11-06 20:21
 */
public class HbaseTest {
    public static void main(String[] args) throws Exception {
        //新建表
        String hbase_table = "test:aaaa";
        HBaseTableUtil.createTable(hbase_table, "cf", true, -1, 1, SpiltRegionUtil.getSplitKeysBydinct());

        //写入数据
        Put put = new Put("1111".getBytes());
        put.addColumn("cf".getBytes(),"aaa".getBytes(),"aaa".getBytes());
        HBaseInsertHelper.put(hbase_table,put);

        //查询
        HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();
        Get get = new Get("1111".getBytes());
        Map<String, String> search = hBaseSearchService.search(hbase_table, get, new MapRowExtrator());


        search.forEach((k,v)->{
            System.out.println(k);
            System.out.println(v);
        });
    }
}
