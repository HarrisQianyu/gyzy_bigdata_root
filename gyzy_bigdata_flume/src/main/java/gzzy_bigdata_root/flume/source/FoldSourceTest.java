/*
package tz14_bigdata_root.flume.source;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static java.io.File.separator;

*/
/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-02-22 21:38
 *//*

public class FoldSourceTest {

    public static void main(String[] args) {
        //TODO ■ 实时解析处理传送过来的FTP格式WIFI数据(txt)

        String foldDir = "F:\\tzjy\\test\\data";
        //列出此目录下的所有文件名
        Collection<File> files = FileUtils.listFiles(new File(foldDir), new String[]{"txt"}, true);
        if(files.size()>0){
            files.forEach(file->{
                String fileName = file.getName();
                //TODO ■ FTP文件备份(数据备份，数据重跑)  最开始的源头是wifi设备。
                //定义备份根目录
                String succDir = "F:\\tzjy\\test\\succ";
                //我们进行备份的时候最好有一个索引，按日期建立目录
                String succDirNew = succDir + separator + "2020-02-22";
                System.out.println("最终文件备份目录" + succDirNew);
                //存放的绝对路径
                String fileNameNew = succDirNew + fileName;

                try {
                    if(new File(fileNameNew).exists()){
                        //判断文件是不是已经存在
                        //如果已经存在，补处理
                    }else{
                        //如果不存在，说明此文件没有被处理过
                        //1.先读取文件内容（是要发送到后面的channel中）
                        List<String> lines = FileUtils.readLines(file);
                        lines.forEach(line->{
                            System.out.println("数据内容-》" + line);
                        });
                        //2。进行备份
                        FileUtils.moveToDirectory(file,new File(succDirNew),true);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //TODO ■ FTP异常文件备份(查错和重跑使用)
                System.out.println(file.getName());
            });
        }


    }
}
*/
