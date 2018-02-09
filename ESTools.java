package com.sun.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ESTools {

	private static Logger logger = LoggerFactory.getLogger(ESTools.class);
	static Settings settings = Settings.builder()
			.put("cluster.name", "esfyb_cluster")
			.put("client.transport.sniff", true).build();
	//创建私有对象
	private static Client client;
	static{
		try {
			logger.info(ESTools.class.getSimpleName()+"开始创建client");
			client = TransportClient
					.builder()
					.settings(settings)
					.build()
					.addTransportAddress(
							new InetSocketTransportAddress(InetAddress
									.getByName("172.16.23.13"), 9303));
			logger.info(ESTools.class.getSimpleName()+"创建Elasticsearch Client 结束");
		} catch (UnknownHostException e2) {
			logger.error(ESTools.class.getSimpleName()+"创建Client异常");
		}	
	}
	
	public static synchronized Client buildclient(){
		return client;
	}
}
