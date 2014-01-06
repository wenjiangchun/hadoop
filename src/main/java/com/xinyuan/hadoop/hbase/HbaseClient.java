package com.xinyuan.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Hbase客户端
 * @author sofar
 *
 */
public class HbaseClient {

	public static void main(String[] args) {
		HBaseAdmin admin = null;
		try {
			Configuration config = HBaseConfiguration.create();
			admin = new HBaseAdmin(config);
			HTableDescriptor table = new HTableDescriptor("test");
			HColumnDescriptor column = new HColumnDescriptor("cf");
			table.addFamily(column);
			admin.createTable(table);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
