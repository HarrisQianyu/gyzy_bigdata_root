package gzzy_bigdata_root.spark.streaming.kafka.warn

import java.util.{Timer, TimerTask}

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import gzzy_bigdata_root.spark.common.SscFactory
import gzzy_bigdata_root.spark.kafka.config.KafkaConfig
import gzzy_bigdata_root.spark.streaming.kafka.utils.KafkaHelper
import gzzy_bigdata_root.spark.warn.timer.SyncRule2RedisTimer

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-03-11 21:16
  */
object WarnStreamingMain extends Serializable with Logging {

  def main(args: Array[String]): Unit = {

    val timer = new Timer
    timer.schedule(new SyncRule2RedisTimer(),0,1*3*1000)

    val topic = "test8"
    val groupId = "WarnStreamingMain"
    val ssc = SscFactory.newLocalSSC("WarnStreamingMain", 5L, 2)
    val kafkaParams = KafkaConfig.getKafkaConfig(groupId)

    val kafkaHelper = new KafkaHelper(kafkaParams, false)
    val DS = kafkaHelper.getMapDSwithOffset(ssc, kafkaParams.asInstanceOf[java.util.Map[String, String]], groupId, topic)
      .persist(StorageLevel.MEMORY_AND_DISK)

    //可以使用并行集合操作

/*    val array = Array("1","2")
    array.par.foreach(x=>{
        if(x==1){
        }else{
        }
    })*/

    //TODO 布控预警
    BlockWarnTask.beginWarn(DS)
    //TODO 流量預警
    FlowWarnTask.beginWarn(DS)

    ssc.start()
    ssc.awaitTermination()

  }

}
