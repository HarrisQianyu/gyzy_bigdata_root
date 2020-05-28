package gzzy_bigdata_root.es.admin;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import gzzy_bigdata_root.common.file.FileCommon;
import gzzy_bigdata_root.es.client.ESclientUtil;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-09 20:15
 */
public class AdminUtil {
    private static final Logger LOG = Logger.getLogger(AdminUtil.class);


    public static void main(String[] args) {
        String path = "es/mapping/wechat.json";
        AdminUtil.buildIndexAndMapping("z5","z5",path,2,1);
    }



    /**
     * 创建索引，同时创建mapping
     * @param index
     * @param type
     * @param path
     * @param shards
     * @param replicas
     * @return
     */
    public static boolean buildIndexAndMapping(String index,
                                               String type,
                                               String path,
                                               int shards,
                                               int replicas){
        boolean flag = true;

        try {
            TransportClient client = ESclientUtil.getClient();
            //先创建索引
            boolean indices = AdminUtil.createIndices(client, index, shards, replicas);
            //创建mapping
            if(indices){
                //如果索引已经创建完成
                LOG.info("索引"+ index + "创建成功");
                String jsonMapping = FileCommon.getAbstractPath(path);
                flag = MappingUtil.addMapping(client,index,type,jsonMapping);
            }else{
                flag = false;
                LOG.error("索引"+ index + "创建失败");
            }
        } catch (Exception e) {
            flag = false;
            LOG.error("索引"+ index + "创建失败",e);
        }


        return flag;
    }



    /**
     * 判断索引是不是已经存在
     * @param client
     * @param index
     * @return
     */
    public static boolean indexExists(TransportClient client,String index){
        boolean tag = true;
        try {
            IndicesExistsResponse indicesExistsResponse = client.admin().indices().prepareExists(index).execute().actionGet();
            tag = indicesExistsResponse.isExists();
        } catch (Exception e) {
            LOG.error(null,e);
        }
        return tag;
    }

    /**
     * ES创建索引
     * @param client
     * @param index
     * @param shards
     * @param replicas
     * @return
     */
    public static boolean createIndices(TransportClient client,
                                        String index,
                                        int shards,
                                        int replicas){
        CreateIndexResponse createIndexResponse = null;
        //判断索引是不是存在
        if(!AdminUtil.indexExists(client,index)){
            createIndexResponse = client.admin().indices().prepareCreate(index)
                    .setSettings(Settings.builder()
                            .put("index.number_of_shards", shards)
                            .put("index.number_of_replicas", replicas))
                    .get();
        }
        return createIndexResponse.isAcknowledged();
    }


}
