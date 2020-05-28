package gzzy_bigdata_root.es.client;

import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import gzzy_bigdata_root.common.config.ConfigUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-07 22:33
 */
public class ESclientUtil{
    private static final Logger LOG = Logger.getLogger(ESclientUtil.class);

    private volatile static TransportClient client;

    //配置文件路径
    private static final String ES_CONFIG_PATH = "es/es_cluster.properties";
    private static Properties properties;

    static {
        properties = ConfigUtil.getInstance().getProperties(ES_CONFIG_PATH);
    }

    private ESclientUtil(){}

    public static TransportClient getClient(){
        try {
            //解决netty冲突
            System.setProperty("es.set.netty.runtime.available.processors", "false");

            String host1 = properties.get("es.cluster.nodes1").toString();
//            String host2 = properties.get("es.cluster.nodes2").toString();
//            String host3 = properties.get("es.cluster.nodes3").toString();
//            String host4 = properties.get("es.cluster.nodes4").toString();
            String cluster_name = properties.get("es.cluster.name").toString();
            Integer tcp_port = Integer.valueOf(properties.get("es.cluster.tcp.port").toString());

            //ES配置
            Settings settings = Settings.builder()
                    .put("cluster.name", cluster_name).build();
            //创建客户端
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(host1), tcp_port));
//                    .addTransportAddress(new TransportAddress(InetAddress.getByName(host2), tcp_port))
//                    .addTransportAddress(new TransportAddress(InetAddress.getByName(host3), tcp_port))
//                    .addTransportAddress(new TransportAddress(InetAddress.getByName(host4), tcp_port));
        } catch (UnknownHostException e) {
            LOG.error("构建ES客户端失败",e);
        }

        return client;
    }

}
