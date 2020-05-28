/**
 * 
 */
package gzzy_bigdata_root.hbase.extractor;

import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;


public class CellNumExtrator implements RowExtractor<Integer> {

	
	public Integer extractRowData(Result result, int rowNum) throws IOException {
		
		return  result.listCells().size();
	}

}
