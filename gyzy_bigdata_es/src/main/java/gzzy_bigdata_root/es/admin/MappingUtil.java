package gzzy_bigdata_root.es.admin;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.transport.TransportClient;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-09 20:42
 */
public class MappingUtil {

    private static final Logger LOG = Logger.getLogger(MappingUtil.class);

    /**
     * 设置mapping
     * @param client
     * @param index
     * @param type
     * @param jsonString
     * @return
     */
    public static boolean addMapping(TransportClient client,
                                     String index,
                                     String type,
                                     String jsonString){
        PutMappingRequest putMappingRequest = new PutMappingRequest(index).type(type).source(JSON.parseObject(jsonString));
        PutMappingResponse putMappingResponse = client.admin().indices().putMapping(putMappingRequest).actionGet();
        boolean acknowledged = putMappingResponse.isAcknowledged();
        return acknowledged;
    }

}
