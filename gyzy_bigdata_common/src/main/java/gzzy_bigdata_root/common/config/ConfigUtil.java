package gzzy_bigdata_root.common.config;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-01 20:16
 */
public class ConfigUtil {
    private static final Logger LOG = Logger.getLogger(ConfigUtil.class);

    //定义一个私有静态变量
    private static volatile ConfigUtil configUtil;

    //私有构造方法
    private ConfigUtil(){}

    //提供一个对外公用方法
    public static ConfigUtil getInstance(){
        //双重否定
        if(configUtil == null){
             //构造
             synchronized (ConfigUtil.class){
                 if(configUtil == null){
                     configUtil = new ConfigUtil();
                 }
             }
        }
        return configUtil;
    }

    /**
     *
     * @param path  配置文件的路径
     * @return
     */
    public Properties getProperties(String path){
        Properties properties = new Properties();

        try {
            InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(path);
            properties.load(resourceAsStream);
        } catch (IOException e) {
            LOG.error("配置文件"+path+"读取失败" ,e);
        }
        return properties;
    }


    /**
     * CompositeConfiguration 工具
     * @param path
     * @return
     */
    public CompositeConfiguration getCompositeConfiguration(String path){
        //配置文件的集合，可以包含多个配置文件
        CompositeConfiguration compositeConfiguration = new CompositeConfiguration();
        PropertiesConfiguration configuration = null;
        try {
            configuration = new PropertiesConfiguration(path);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        compositeConfiguration.addConfiguration(configuration);
        return compositeConfiguration;
    }




    public static void main(String[] args) {
        String path = "kafka/kafka-server-config.properties";
        Properties properties = ConfigUtil.getInstance().getProperties(path);
        properties.keySet().forEach(key->{
            System.out.println(key);
            System.out.println(properties.getProperty(key.toString()));
        });
    }


}
