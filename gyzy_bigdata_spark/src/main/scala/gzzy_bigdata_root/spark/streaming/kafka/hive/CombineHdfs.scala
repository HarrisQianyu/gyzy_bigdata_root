package gzzy_bigdata_root.spark.streaming.kafka.hive

import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import gzzy_bigdata_root.spark.common.SessionFactory
import gzzy_bigdata_root.spark.hive.HdfsAdmin

import scala.collection.JavaConversions._
/**
  * @author: KING
  * @description: 小文件合并
  * @Date:Created in 2020-03-16 22:14
  */
object CombineHdfs extends Serializable with Logging {

  def main(args: Array[String]): Unit = {
      // 1 获取HDFS连接
    val spark =  SessionFactory.newLocalHiveSession("CombineHdfs",2)
      // 2. 读取所有文件
    //按类型合并，遍历数据类型
    HiveConfig.tables.foreach(table=>{
      println("============开始进行小文件合并任务=================")
      //HDFS路径
      val table_path = s"hdfs://cdh01:8020${HiveConfig.rootPath}/${table}"
      val tableDF = spark.read.parquet(table_path)


      // 获取到所有的小文件名
      val fileSystem:FileSystem = HdfsAdmin.get().getFs
      val fileStatusArray:Array[FileStatus] = fileSystem.globStatus(new Path(table_path+"/part*"))
      //将状态转为文件路径
      val paths = FileUtil.stat2Paths(fileStatusArray)

      paths.foreach(path=>{
         println(s"=====${path}=======")
      })
      //3. 合并成一个文件,重新写回原目录
      tableDF.repartition(1).write.mode(SaveMode.Append).parquet(table_path)
      //合并之后的文件已经写入了

      // 4.删除原来的小文件  用到hdfs工具类
      paths.foreach(path=>{
        fileSystem.delete(path)
        println(s"删除小文件${path}成功")
      })
    })

  }
}
