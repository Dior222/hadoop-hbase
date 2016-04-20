package com.sinovatio.fulltext.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

public class PutMerge {

	public static void put(String localDir, String hdfsFile) throws IOException {
		Configuration conf = new Configuration();
		Path localPath = new Path(localDir) ;
		Path hdfsFilepath = new Path(hdfsFile) ;
		FSDataOutputStream oos = null ;
		FSDataInputStream dis = null ;
		try {
			FileSystem localFs = FileSystem.getLocal(conf) ;
			FileSystem hdfsFs = FileSystem.get(conf) ;
			
			//create a merged hdfs file
			oos = hdfsFs.create(hdfsFilepath) ;
			
			//list all files in local directory
			FileStatus[] status = localFs.listStatus(localPath, new LogFilePathFilter()) ;
			System.out.println(status);
			try {
				byte[] data = new byte[1024] ;
				int len = 0 ;
				for(FileStatus fileStatus : status) {
					Path path = fileStatus.getPath() ;
					System.out.println("found file : " + path.getName());
					dis = localFs.open(path) ;
					while((len = dis.read(data)) > 0) {
						oos.write(data);
					}
				}
				System.out.println("merge file ok.");
				System.out.println("hdfs merged file is : " + hdfsFile);
			} finally {
				IOUtils.closeStream(dis);
			}
			System.out.println("write content ok.");
		} finally {
			IOUtils.closeStream(oos);
		}
	}
	
	static class LogFilePathFilter implements PathFilter {

		public boolean accept(Path path) {
			return path.getName().endsWith("log");
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		String localDir = "/opt/modules/hadoop-1.2.1/logs" ;
		String hdfsFile = "/wc/merge.log" ;
		put(localDir, hdfsFile);

	}

}
