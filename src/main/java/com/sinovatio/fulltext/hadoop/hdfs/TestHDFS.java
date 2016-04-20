package com.sinovatio.fulltext.hadoop.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class TestHDFS {

	/**
	 * URL way
	 */
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	/**
	 * read file content
	 * @param filepath
	 * @throws IOException
	 */
	public static void read(String filepath) throws IOException {
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
//		Path path = new Path(filepath) ;
		InputStream in = null;
		try {
//			in = fs.open(path);
			in = new URL(filepath).openStream() ;
			IOUtils.copyBytes(in, System.out, 4096);
		} finally {
			IOUtils.closeStream(in);
		}

	}
	
	/**
	 * create a new file
	 * @param filename
	 * @param content
	 * @throws IOException
	 */
	public static void create(String filename, byte[] content) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filename) ;
		FSDataOutputStream oos = null ;
		try {
			oos = fs.create(path) ;
			oos.write(content);
			System.out.println("write content ok.");
		} finally {
			IOUtils.closeStream(oos);
		}
	}
	
	/**
	 * create a new file
	 * @param filename
	 * @param content
	 * @throws IOException
	 */
	public static void upload(String src, String dst) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(src) ;
		Path dstPath = new Path(dst) ;
		FSDataOutputStream oos = null ;
		try {
			fs.copyFromLocalFile(false, srcPath, dstPath);
			System.out.println("upload data ok.");
			System.out.println("list files in : " + conf.get("fs.default.name"));
			FileStatus[] fileStatus = fs.listStatus(dstPath) ;
			for(FileStatus fileStatu : fileStatus) {
				System.out.println(fileStatu.getPath());
			}
			
		} finally {
			IOUtils.closeStream(oos);
		}
	}
	
	public static void main(String[] args) throws IOException {
		read("hdfs://localhost:9000/wc/input/hdfs-site.xml");
//		read("/wc/input/hdfs-site.xml");
//		create("/wc/test_create.txt", "hello hadoop".getBytes());
//		String src = "/home/hadoop/1.txt" ;
//		String dst = "/wc/" ;
//		upload(src, dst);

	}

}
