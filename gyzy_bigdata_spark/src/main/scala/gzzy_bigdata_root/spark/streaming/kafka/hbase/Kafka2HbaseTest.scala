package gzzy_bigdata_root.spark.streaming.kafka.hbase

import java.util
import java.util.List

import org.apache.hadoop.hbase.client.Put
import org.apache.spark.internal.Logging
import gzzy_bigdata_root.hbase.config.HBaseTableUtil
import gzzy_bigdata_root.hbase.insert.HBaseInsertHelper
import gzzy_bigdata_root.hbase.spilt.SpiltRegionUtil
import gzzy_bigdata_root.spark.common.SscFactory
import gzzy_bigdata_root.spark.kafka.config.KafkaConfig
import gzzy_bigdata_root.spark.streaming.kafka.utils.KafkaHelper

import scala.collection.JavaConversions._

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-03-20 20:16
  */
object Kafka2HbaseTest extends Serializable with Logging{

  def main(args: Array[String]): Unit = {


    //新建表
    val hbase_table = "test:bbbb"
    HBaseTableUtil.createTable(hbase_table, "cf", true, -1, 1, SpiltRegionUtil.getSplitKeysBydinct)


    val topic = "test8"
    val groupId = "Kafka2HbaseTest"

    val ssc = SscFactory.newLocalSSC("Kafka2HbaseTest", 5L,2)
    val kafkaParams = KafkaConfig.getKafkaConfig(groupId)

    val kafkaHelper = new KafkaHelper(kafkaParams, false)
    val DS = kafkaHelper.getMapDSwithOffset(ssc, kafkaParams.asInstanceOf[java.util.Map[String, String]], groupId, topic)

    DS.foreachRDD(rdd=>{

      rdd.foreachPartition(partion=>{

        val puts: util.List[Put] = new util.ArrayList[Put]()
        while (partion.hasNext){
          val map = partion.next()
          val hbaseKey = map.get("id").toString
          val put = new Put(hbaseKey.getBytes())
          val keys = map.keySet()
          keys.foreach(key=>{
            //写入列
            put.addColumn("cf".getBytes,key.toString.getBytes(),map.get(key).toString.getBytes())
          })
          puts.add(put)
        }
        HBaseInsertHelper.put(hbase_table,puts)
      })
    })


    ssc.start()
    ssc.awaitTermination()

  }

}
