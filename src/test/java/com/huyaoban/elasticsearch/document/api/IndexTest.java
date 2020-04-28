package com.huyaoban.elasticsearch.document.api;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import lombok.extern.slf4j.Slf4j;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class IndexTest {
	@Autowired
	private RestHighLevelClient restHighLevelClient;

	@Test
	public void test1() {
		// 索引名称
		IndexRequest request = new IndexRequest("posts");
		// 指定文档ID
		request.id("1");

		String jsonString = "{" + "\"user\":\"kimchy\"," + "\"postDate\":\"2013-01-30\","
				+ "\"message\":\"trying out Elasticsearch\"" + "}";
		request.source(jsonString, XContentType.JSON);

		try {
			IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
			log.info("{}", response);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test2() {
		Map<String, Object> jsonMap = new HashMap<>();
		jsonMap.put("user", "huyaoban");
		jsonMap.put("postDate", new Date());
		jsonMap.put("message", "trying out Elasticsearch");

		IndexRequest request = new IndexRequest("posts").id("2").source(jsonMap);

		try {
			IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
			log.info("{}", response);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test3() throws IOException {
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		{
			builder.field("user", "jerry.hu");
			builder.timeField("postDate", new Date());
			builder.field("message", "trying out Elasticsearch");
		}
		builder.endObject();

		IndexRequest request = new IndexRequest("posts").id("3").source(builder);

		try {
			IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
			log.info("{}", response);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test4() throws IOException {
		IndexRequest request = new IndexRequest("posts").id("4").source("user", "kimchy", "postDate",
				new Date(),
				"message", "trying out Elasticsearch");

		try {
			IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
			log.info("{}", response);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
