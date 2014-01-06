package com.xinyuan.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xinyuan.hadoop.exception.UndefinedException;
import com.xinyuan.hadoop.utils.HadoopUtils;

/**
 * Hdfs客户端工具类
 * 
 * @author sofar
 *
 */
public class HdfsClient {

	private static final Logger logger = LoggerFactory.getLogger(HdfsClient.class);
	
	/**
     * 从本地文件系统向hdfs文件系统写入文件
     * @param sourceFile 本地文件
     * @param destinationFile 目标hdfs文件
     * @throws IOException
     */
    public static void write(String sourceFile, String destinationFile) throws IOException {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(destinationFile), conf);
            fs.copyFromLocalFile(new Path(sourceFile), new Path(destinationFile));
            logger.debug("copy From Local File to hdfs, local file={}, hdfs file={}", sourceFile, destinationFile);
    }
    
    /**
     * 读取Hdfs文件
     * @param hdfsPath Hdfs文件路径
     * @throws IOException
     */
    public static void read(String hdfsPath) throws IOException {
            Configuration conf = new Configuration();
            FSDataInputStream in = null;
            FileSystem fs = null;
            logger.debug("starting read.... ");
            try {
             fs = FileSystem.get(URI.create(hdfsPath), conf);
             in = fs.open(new Path(hdfsPath));
             logger.debug(in.toString());
             logger.debug("ending read.... ");
            } catch (IOException e) {
                    e.printStackTrace();
            } finally {
                    IOUtils.closeStream(in);
                    fs.close();
            }
    }
    public static void main(String[] args) throws UndefinedException, IOException {
    	String sourceFile = "/home/sofar/下载/1.txt";
    	String destinationFile = HadoopUtils.getHdfsPath() + "/books/1.txt";
		//HdfsClient.write(sourceFile, destinationFile);
    	HdfsClient.read(destinationFile);
	}
}
