package com.sinovatio.fulltext.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TestScanner {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "testtable");
		Scan scan = new Scan() ;
		scan.addFamily(Bytes.toBytes("colfam1")) ;
		ResultScanner scanner = table.getScanner(scan) ;
		for(Result result : scanner) {
			System.out.println(result);
		}
		scanner.close();
		

	}

}
