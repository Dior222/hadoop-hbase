package com.sinovatio.fulltext.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 测试get的用法
 * @author darwin
 *
 */
public class TestHBaseGet {

	/**
	 * 最基本的用法
	 * @throws IOException
	 */
	public static void testGet() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "testtable");
		Get get = new Get(Bytes.toBytes("col1")) ;
		get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")) ;
		Result result = table.get(get) ;
		System.out.println("Result : " + result);
		byte[] valueByte = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")) ;
		System.out.println("Value : " + Bytes.toString(valueByte));
	}
	
	public static void main(String[] args) throws IOException {
		testGet();

	}

}
