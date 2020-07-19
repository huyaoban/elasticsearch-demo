package com.huyaoban.elasticsearch.document.api;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class GetApiTest {
	@Autowired
	private RestHighLevelClient restHighLevelClient;

	@Test
	public void test1() throws IOException {
		GetRequest request = new GetRequest("posts", "1");
		GetResponse response1 = restHighLevelClient.get(request, RequestOptions.DEFAULT);
		printGetResponse(response1);

		GetRequest request2 = new GetRequest("posts", "does_not_exists");
		GetResponse response2 = restHighLevelClient.get(request2, RequestOptions.DEFAULT);
		printGetResponse(response2);
	}

	//@Test
	public void test3() throws IOException {
		GetRequest request = new GetRequest("posts", "1");

		//设置不返回source字段
		request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);

		//指定_source里面返回的字段(只返回message和*Date结尾的字段)
		String[] includes = new String[]{"message", "*Date"};
		String[] excludes = Strings.EMPTY_ARRAY;
		FetchSourceContext fetchSourceContext =
				new FetchSourceContext(true, includes, excludes);
		request.fetchSourceContext(fetchSourceContext);

		//指定不返回某些字段，不返回message字段
		String[] includes1 = Strings.EMPTY_ARRAY;
		String[] excludes1 = new String[]{"message"};
		FetchSourceContext fetchSourceContext1 =
				new FetchSourceContext(true, includes1, excludes1);
		request.fetchSourceContext(fetchSourceContext1);

		//设置偏好分片，优先使用该分片搜索
		request.preference("preference");
		//设置是否从translog读取
		request.realtime(false);

		//取回文档前，触发refresh操作将内存缓存种的内容刷到文件系统缓存
		request.refresh(true);

		//取回指定版本的文档
		request.version(2);
	}

	@Test
	public void test4() throws IOException {
		//索引不存在时抛异常
		GetRequest request1 = new GetRequest("does_not_exist", "1");
		try {
			GetResponse response1 = restHighLevelClient.get(request1, RequestOptions.DEFAULT);
		} catch (ElasticsearchException e) {
			if(e.status() == RestStatus.NOT_FOUND) {
				log.error("索引不存在");
			}
		}

		//取指定版本的文档，不存在时抛异常
		GetRequest request2 = new GetRequest("posts", "1").version(5);
		try {
			GetResponse response2 = restHighLevelClient.get(request2, RequestOptions.DEFAULT);
		} catch (ElasticsearchException e) {
			if(e.status() == RestStatus.CONFLICT) {
				log.error("版本冲突");
			}
		}
	}

	private void printGetResponse(GetResponse getResponse) {
		String index = getResponse.getIndex();
		String id = getResponse.getId();
		if(getResponse.isExists()) {
			log.info("文档存在");
			long version = getResponse.getVersion();
			String sourceAsString = getResponse.getSourceAsString();
			Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
			byte[] sourceAsBytes = getResponse.getSourceAsBytes();
		} else {
			//文档不存在
			log.warn("文档不存在");
		}

	}

}
