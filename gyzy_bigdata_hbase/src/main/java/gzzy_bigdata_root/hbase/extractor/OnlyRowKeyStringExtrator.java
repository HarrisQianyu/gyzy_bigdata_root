package gzzy_bigdata_root.hbase.extractor;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class OnlyRowKeyStringExtrator implements RowExtractor<String> {

	
	
	public String extractRowData(Result result, int rowNum) throws IOException {
		
		return Bytes.toString( result.getRow() );
	}

}
