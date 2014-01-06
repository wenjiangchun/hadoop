package com.xinyuan.hadoop.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xinyuan.hadoop.exception.UndefinedException;

public class HadoopUtils {
	private static Logger logger = LoggerFactory.getLogger(HadoopUtils.class);

    private static Map<String, Object> hadoopMap = new HashMap<String, Object>();
    
    static {
            Properties p = new Properties();
            try {
                    p.load(ClassLoader.getSystemResourceAsStream("hadoop.properties"));
                    logger.debug("starting load hadoop.properties!");
                    for (Object key : p.keySet()) {
                            hadoopMap.put((String) key, p.get(key));
                            logger.debug("adding hadoop property, key={},value={}", key, p.get(key));
                    }
            } catch (FileNotFoundException e) {
                    logger.error("FileNotFoundException,error={}", e.getMessage());
                    e.printStackTrace();
            } catch (IOException e) {
                    logger.error("IOException,error={}", e.getMessage());
                    e.printStackTrace();
            }
            logger.debug("load hadoop.properties completed!");
    }
    
    public static Map<String, Object> getHadoopMap() {
            return hadoopMap;
    }
    
    /**
     * 获取HDFS路径信息
     * @return HDFS路径信息
     * @throws UndefinedException
     */
    public static String getHdfsPath() throws UndefinedException {
            if (hadoopMap.containsKey("hdfs.path")) {
                    return (String) hadoopMap.get("hdfs.path");
            } else {
                    logger.error("'hdfs.path' is not defined, please define it in the 'hadoop.properties' file ");
                    throw new UndefinedException("'hdfs.path' is not defined, please define it in the 'hadoop.properties'");
            }
    }
    
    /**
     * 获取类资源根路径 如resource目录和Java目录路径
     * @return 类资源加载根目录
     */
    public static String getSystemRootPath() {
            return ClassLoader.getSystemResource("").getPath();
    }
}
