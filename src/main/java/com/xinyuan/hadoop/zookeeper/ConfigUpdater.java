package com.xinyuan.hadoop.zookeeper;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUpdater {

	private static final Logger logger = LoggerFactory.getLogger(ConfigUpdater.class);
	
	public static final String PATH = "/config";
	private ZooKeeperUtils utils;
	private Random random = new Random();
	
	public ConfigUpdater(String hosts) throws IOException, InterruptedException {
		utils = new ZooKeeperUtils();
		utils.connect(hosts);
	}
	
	public void run() throws InterruptedException {
		while(true) {
			String value = random.nextInt(100) + "";
			utils.write(PATH, value);
			logger.debug("将值{}写入到{}节点路径中。", value, PATH);
			TimeUnit.SECONDS.sleep(random.nextInt(10));
		}
	}

	public static void main(String[] args) {
		ConfigUpdater updater;
		try {
			updater = new ConfigUpdater("localhost");
			updater.run();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		
	}

}
