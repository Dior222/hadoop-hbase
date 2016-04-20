package com.sinovatio.fulltext.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 测试hbase数据的插入
 * 
 * @author darwin
 *
 */
public class TestHBase {

	/**
	 * autoflash=false
	 * @throws IOException
	 */
	public static void testAutoFlush() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "testtable");
		System.out.println("auto flush : " + table.isAutoFlush());
		table.setAutoFlush(false);
		Put put1 = new Put(Bytes.toBytes("col2"));
		put1.add(Bytes.toBytes("colfam2"), Bytes.toBytes("qual11"), Bytes.toBytes("val11"));
		table.put(put1);

		Put put2 = new Put(Bytes.toBytes("col2"));
		put2.add(Bytes.toBytes("colfam2"), Bytes.toBytes("qual12"), Bytes.toBytes("val12"));
		table.put(put2);

		Put put3 = new Put(Bytes.toBytes("col2"));
		put3.add(Bytes.toBytes("colfam2"), Bytes.toBytes("qual13"), Bytes.toBytes("val13"));
		table.put(put3);

		Get get = new Get(Bytes.toBytes("col2"));
		Result result1 = table.get(get);
		System.out.println("result1 : " + result1);

		table.flushCommits();

		Result result2 = table.get(get);
		System.out.println("result1 : " + result2);

	}

	/**
	 * 批量插入
	 * @throws IOException
	 */
	public static void putList() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "testtable");

		List<Put> puts = new ArrayList<Put>();
		for (int i = 10; i < 20; i++) {
			Put put = new Put(Bytes.toBytes("col1"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual" + i), Bytes.toBytes("val" + i));
			puts.add(put);
		}
		table.put(puts);
	}

	/**
	 * 测试最基本的添加数据
	 * @throws IOException
	 */
	private static void testPut() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "testtable");
		Put put = new Put(Bytes.toBytes("col1"));
		for (int i = 0; i < 10; i++) {
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual" + i), Bytes.toBytes("val" + i));
		}
		table.put(put);
	}

	/**
	 * 测试checkAndPut方法
	 * @throws IOException
	 */
	public static void checkAndPut() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "testtable");
		Put put = new Put(Bytes.toBytes("col1"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
		boolean result = table.checkAndPut(Bytes.toBytes("col1"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
				null, put);
		System.out.println("put aplied : " + result);//false
	}

	/**
	 * 方法调用
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// testPut();
		// testAutoFlush();
//		putList();
		checkAndPut();

	}

}
