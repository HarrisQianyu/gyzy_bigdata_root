package gzzy_bigdata_root;

import org.elasticsearch.client.transport.TransportClient;
import gzzy_bigdata_root.es.client.ESclientUtil;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
//        LinkedList<Integer> integers = new LinkedList<>();
//        integers.offerLast(1);
//        integers.offerLast(2);
//        System.out.println(integers.getLast());
//        ArrayList a = new ArrayList();
//
//        System.out.println( "Hello World!" );
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
//        String format = simpleDateFormat.format(new Date());
//        System.out.println(format);
        TransportClient client = ESclientUtil.getClient();

    }
}
