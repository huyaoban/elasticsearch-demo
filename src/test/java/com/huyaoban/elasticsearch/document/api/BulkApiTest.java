package com.huyaoban.elasticsearch.document.api;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class BulkApiTest {
	@Autowired
	private RestHighLevelClient restHighLevelClient;

	@Test
	public void test1() throws IOException {
		BulkRequest bulkRequest = new BulkRequest("posts");
		bulkRequest.add(new IndexRequest("posts").id("1")
				.source("message", "hello world1", "postDate", "2020-07-19", "user", "jerry.hu"));
		bulkRequest.add(new IndexRequest("posts").id("2")
				.source("message", "hello world2", "postDate", "2020-07-19", "user", "jerry.hu"));
		bulkRequest.add(new IndexRequest("posts").id("3")
				.source("message", "hello world3", "postDate", "2020-07-19", "user", "jerry.hu"));

		BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
		printBulkResponse(bulkResponse);
	}

	private void printBulkResponse(BulkResponse response) {
		if(response.hasFailures()) {
			log.error("有些操作失败");
		}

		//遍历
		for(BulkItemResponse bulkItemResponse : response) {
			DocWriteResponse itemResponse = bulkItemResponse.getResponse();
			if(bulkItemResponse.isFailed()) {
				//处理失败的请求
				bulkItemResponse.getFailure();
				log.error("失败的请求");
			}

			switch(bulkItemResponse.getOpType()) {
				case INDEX:
				case CREATE:
					IndexResponse indexResponse = (IndexResponse)itemResponse;
					break;
				case UPDATE:
					UpdateResponse updateResponse = (UpdateResponse)itemResponse;
					break;
				case DELETE:
					DeleteResponse deleteResponse = (DeleteResponse)itemResponse;
					break;
			}
		}
	}
}
