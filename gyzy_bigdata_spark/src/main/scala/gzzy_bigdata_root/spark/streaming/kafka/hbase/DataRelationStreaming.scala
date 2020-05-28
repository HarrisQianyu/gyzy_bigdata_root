package gzzy_bigdata_root.spark.streaming.kafka.hbase

import org.apache.hadoop.hbase.client.Put
import org.apache.spark.internal.Logging
import gzzy_bigdata_root.hbase.config.HBaseTableUtil
import gzzy_bigdata_root.hbase.insert.HBaseInsertHelper
import gzzy_bigdata_root.hbase.spilt.SpiltRegionUtil
import gzzy_bigdata_root.spark.common.SscFactory
import gzzy_bigdata_root.spark.kafka.config.KafkaConfig
import gzzy_bigdata_root.spark.streaming.kafka.utils.KafkaHelper

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-03-20 20:33
  */
object DataRelationStreaming extends Serializable with Logging{


  def main(args: Array[String]): Unit = {

    //首先定义关联字段
    val arrayFields = Array("relation","phone","wechat","qq","send_mail","username")
    val hbase_table = "test:relation"
    initHbaseTable(arrayFields)

    val topic = "test8"
    val groupId = "DataRelationStreaming"
    val ssc = SscFactory.newLocalSSC("DataRelationStreaming", 5L, 2)
    val kafkaParams = KafkaConfig.getKafkaConfig(groupId)

    val kafkaHelper = new KafkaHelper(kafkaParams, false)
    val DS = kafkaHelper.getMapDSwithOffset(ssc, kafkaParams.asInstanceOf[java.util.Map[String, String]], groupId, topic)

    DS.foreachRDD(rdd=>{
        rdd.foreachPartition(partion=>{
          while (partion.hasNext){
            val map = partion.next()
            println(map)
            //TODO 主表
            val phone_mac = map.get("phone_mac")  //主表key
            arrayFields.foreach(relationField=>{
              if(map.containsKey(relationField)){
                //说明有关联字段
                val rowKey = phone_mac.getBytes()  //主键
                val col = relationField.getBytes() // 列名
                val value = map.get(relationField).getBytes()//值
                val put = new Put(rowKey)
                //构造版本号 版本号必须为正整数
               val ts = (relationField+ map.get(relationField)).hashCode & Integer.MAX_VALUE
                put.addColumn("cf".getBytes(),col,ts,value)
                HBaseInsertHelper.put(hbase_table,put)

                //TODO 索引表
                val sort_tableName=s"test:${relationField}"
                val sort_Rowkey = value  //索引边的ROWKEY 就是主表的字段值
                val sort_put = new Put(sort_Rowkey)
                val sort_family = "cf".getBytes()
                val sort_qualifier = "phone_mac".getBytes()
                val sort_value = rowKey
                //构造版本号 版本号必须为正整数
                val sort_ts = (map.get(relationField)).hashCode & Integer.MAX_VALUE
                sort_put.addColumn(sort_family,sort_qualifier,sort_ts,sort_value)
                HBaseInsertHelper.put(sort_tableName,sort_put)
              }
            })
          }
        })
    })
    ssc.start()
    ssc.awaitTermination()

  }

  def initHbaseTable(arrayFields:Array[String]): Unit ={
    arrayFields.foreach(table=>{
      HBaseTableUtil.createTable(s"test:${table}", "cf", true, -1, 1000, SpiltRegionUtil.getSplitKeysBydinct)

      //HBaseTableUtil.deleteTable(s"test:${table}")
    })
  }


}
