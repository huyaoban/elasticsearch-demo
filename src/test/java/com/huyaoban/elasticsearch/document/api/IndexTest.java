package com.huyaoban.elasticsearch.document.api;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
			printIndexResponse(response);
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
			printIndexResponse(response);
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

	/**
	 * Request可设置的属性
	 */
	public void test5() {
		IndexRequest request = new IndexRequest("posts");

		//指定分片路由值
		request.routing("routing");

		//设置超时时间（写主分片的超时时间）
		request.timeout(TimeValue.timeValueSeconds(1L));
		request.timeout("1s");

		request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
		request.setRefreshPolicy("wait_for");

		request.version(2L);
		request.versionType(VersionType.EXTERNAL);

		//INDEX:索引文档，如果存在相同ID文档，则替换旧文档
		//CREATE:索引文档，如果存在相同ID文档，则抛异常。异常的status为CONFLICT
		request.opType(DocWriteRequest.OpType.CREATE);
		request.opType("create");

		request.setPipeline("pipeline");
	}

	/**
	 * 异步调用接口
	 */
	@Test
	public void test6() {
		// 索引名称
		IndexRequest request = new IndexRequest("posts");
		// 指定文档ID
		request.id("10");

		String jsonString = "{" + "\"user\":\"kimchy\"," + "\"postDate\":\"2013-01-30\","
				+ "\"message\":\"trying out Elasticsearch\"" + "}";
		request.source(jsonString, XContentType.JSON);

		ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {

			@Override
			public void onResponse(IndexResponse response) {
				log.info("successful: {}", response);
			}

			@Override
			public void onFailure(Exception e) {
				log.info("exception", e);
			}
		};

		//异步调用不知道为什么一直有问题
		restHighLevelClient.indexAsync(request, RequestOptions.DEFAULT, listener);
	}

	private void printIndexResponse(IndexResponse indexResponse) {
		String index = indexResponse.getIndex();
		String id = indexResponse.getId();
		if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
			log.info("document created success");
		} else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
			log.info("document updated success");
		}

		ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
		if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
			log.warn("有部分分片没有同步数据");
		}

		if (shardInfo.getFailed() > 0) {
			for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
				String reason = failure.reason();
				log.warn("失败原因：{}", reason);
			}
		}
	}

	@Test
	public void test7() throws IOException {
		// 索引名称
		IndexRequest request = new IndexRequest("posts");

		//存在ID相同的文档时会抛异常
		request.opType(DocWriteRequest.OpType.CREATE);

		// 指定文档ID
		request.id("1");

		String jsonString = "{" + "\"user\":\"kimchy\"," + "\"postDate\":\"2013-01-30\","
				+ "\"message\":\"trying out Elasticsearch\"" + "}";
		request.source(jsonString, XContentType.JSON);

		try {
			IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
			log.info("{}", response);
			printIndexResponse(response);
		} catch (ElasticsearchException e) {
			e.printStackTrace();
			if(e.status() == RestStatus.CONFLICT) {
				log.warn("已存在相同文档");
			}
		}
	}
}
