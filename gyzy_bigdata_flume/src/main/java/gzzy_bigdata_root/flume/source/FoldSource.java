package gzzy_bigdata_root.flume.source;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;
import gzzy_bigdata_root.flume.constant.*;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.io.File.separator;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-02-22 21:36
 */
public class FoldSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger LOG = Logger.getLogger(FoldSource.class);

    //抽取配置参数
    String folderDir;         //文件监控目录
    String succDir;           //备份目录
    String  errorDir;         //错误文件备份
    private int fileNum;      // 每批次处理的文件数量
    private List<File> listFiles; //存放每批次文件的集合
    private List<Event> listEvents; //存放每个文件的所有记录

    @Override
    public void configure(Context context) {
        //参数提取
        listEvents = new ArrayList<>();
        folderDir = context.getString(FlumeParamConstant.FOLDER_DIR);
        succDir = context.getString(FlumeParamConstant.SUCC_DIR);
        errorDir = context.getString(FlumeParamConstant.ERROR_DIR);
        fileNum = context.getInteger(FlumeParamConstant.FILE_NUM);
        LOG.info("获取folderDir" + folderDir);
        LOG.info("获取succDir" + succDir);
        LOG.info("获取errorDir" + errorDir);
        LOG.info("获取fileNum" + fileNum);
    }

    @Override
    public Status process() throws EventDeliveryException {

        //主要是便于观察日志。
//        try {
//            Thread.currentThread().sleep(2000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        Status status = null;
        //TODO ■ 实时解析处理传送过来的FTP格式WIFI数据(txt)
        try {
            //列出此目录下的所有文件名
            List<File> files = (List<File>)FileUtils.listFiles(new File(folderDir), new String[]{"txt"}, true);

            //当文件数量特别多的时间可能导致问题，这里会进行小批量处理
            int fileCount = files.size();
            if(fileCount > fileNum){
                 //用一个集合来存放截取的文件
                listFiles = files.subList(0,fileNum);
            }else{
                //如果小于fileNUm
                listFiles = files;
            }

            if(listFiles.size()>0){
                for (int i = 0; i < listFiles.size() ; i++) {
                    LOG.info("文件数："+listFiles);
                    File file = listFiles.get(i);
                    String fileName = file.getName();
                    //TODO ■ FTP文件备份(数据备份，数据重跑)  最开始的源头是wifi设备。
                    //定义备份根目录

                    //我们进行备份的时候最好有一个索引，按日期建立目录
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    String curTime = simpleDateFormat.format(new Date());
                    String succDirNew = succDir + separator + curTime;
                    String errorDirNew = errorDir + separator + curTime;

                    System.out.println("最终文件备份目录" + succDirNew);
                    //存放的绝对路径
                    String fileNameNew = succDirNew + fileName;
                    try {
                        if(new File(fileNameNew).exists()){
                            //判断文件是不是已经存在
                            //如果已经存在，补处理
                            LOG.info("文件已经存在，不处理："+fileNameNew);
                        }else{
                            //如果不存在，说明此文件没有被处理过
                            //1.先读取文件内容（是要发送到后面的channel中）
                            List<String> lines = FileUtils.readLines(file);

                            //批量推送
                            lines.forEach(line->{
                                // flume进行数据传输的时候 都是封装为event进行传送的
                                Event e = new SimpleEvent();
                                e.setBody(line.getBytes());
                                //设置消息头。可以设置一些说明，参数之类
                                //可以把当前文件和 移动之后存放的地址传送到channel
                                Map headers = new HashMap<String,String>();
                                headers.put(ConstantFields.FILE_NAME,fileName);
                                headers.put(ConstantFields.ABSOLUTE_FILENAME,fileNameNew);
                                e.setHeaders(headers);
                                listEvents.add(e);
                                LOG.info("数据内容-》" + line);

                            });
                            //2。进行备份
                            FileUtils.moveToDirectory(file,new File(succDirNew),true);
                            LOG.info("file"+file.getName()+"备份到成功目录->:"+succDirNew);
                        }
                    } catch (IOException e) {
                        LOG.error(e.getMessage());
                        LOG.info("file"+file.getName()+"备份到失败目录->:"+errorDirNew);
                        FileUtils.moveToDirectory(file,new File(errorDirNew),true);
                        //TODO ■ FTP异常文件备份(查错和重跑使用)
                       LOG.error(null,e);

                    }
                }
                //批量推送  需要一个list<Event>
                getChannelProcessor().processEventBatch(listEvents);
                listEvents.clear();
            }
            status = Status.READY;
        } catch (Exception e) {
            status = Status.BACKOFF;
            e.printStackTrace();
        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


}
