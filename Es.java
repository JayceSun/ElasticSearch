package com.sun.utils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Es {

	private static Logger logger = LoggerFactory.getLogger(Es.class);
	private static String loggerName = Es.class.getSimpleName();

	@SuppressWarnings("rawtypes")
	public static void setData(List<String> json,String topic ) {

		Client client = ESTools.buildclient();

		String indexName = "xrs_db";
//
		BulkRequestBuilder prepareBulk = client.prepareBulk();
		
		Iterator it = json.iterator();
		while(it.hasNext()){
			
			try {
				IndexRequestBuilder indexRequest = client.prepareIndex(indexName, topic).setSource(it.next().toString());
				prepareBulk.add(indexRequest);
			} catch (Exception e) {
				System.out.println(loggerName+"--数据add出错啦:"+e.getMessage());
			}
		}
		
//		Iterator iter = topic.entrySet().iterator();
		
//		while (iter.hasNext()) {
//			Map.Entry entry = (Map.Entry) iter.next();
//			String key = (String) entry.getKey();
//			String val = (String) entry.getValue();
//			
//			try {
//				IndexRequestBuilder indexRequest = client.prepareIndex(indexName, val).setSource(key);
//				prepareBulk.add(indexRequest);
//			} catch (Exception e) {
//				logger.error(loggerName+"--数据add出错啦:"+e.getMessage());
//			}
//			
		
//		}

		BulkResponse bulkResponse = prepareBulk.execute().actionGet();
        if (bulkResponse.hasFailures()){
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
               System.out.println(loggerName+"--数据提交出错啦:"+bulkItemResponse.getFailureMessage());
            }
        }
	}
}
