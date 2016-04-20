package com.sinovatio.fulltext.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

public class TestBatch {

	private static final byte[] COL = Bytes.toBytes("col1") ;
	private static final byte[] FAMILY = Bytes.toBytes("colfam1") ;
	private static final byte[] QUALIFIER = Bytes.toBytes("qual1") ;
	
	/**
	 * 批量操作
	 * @throws IOException
	 */
	public static void testBatch() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "testtable");
		List<Row> rows = new ArrayList<Row>();
		//put
		Put put = new Put(COL) ;
		put.add(FAMILY, QUALIFIER, Bytes.toBytes("val100")) ;
		rows.add(put) ;
		
		//delete
		Delete del = new Delete(COL) ;
		del.deleteColumn(FAMILY, QUALIFIER) ;
		rows.add(del) ;
		
		//get
		Get get = new Get(COL);
		get.addFamily(Bytes.toBytes("colfam1")) ;
		rows.add(get);
		
		Object[] results = new Object[rows.size()] ;
		try {
			//results = table.batch(rows);
			table.batch(rows, results) ;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		for(int i = 0 ; i < results.length ; i++) {
			System.out.println("results[" + i + "]:" + results[i]);
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		testBatch();

	}

}
