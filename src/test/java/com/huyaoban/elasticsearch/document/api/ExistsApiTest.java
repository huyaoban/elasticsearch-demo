package com.huyaoban.elasticsearch.document.api;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class ExistsApiTest {
	@Autowired
	private RestHighLevelClient restHighLevelClient;

	@Test
	public void test1() throws IOException {
		GetRequest request = new GetRequest("posts", "1");
		//exists api只返回true和false，为了让请求更轻量，设置不返回source和storedFields
		//设置不返回source字段
		request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
		request.storedFields("_none_");
		boolean exists = restHighLevelClient.exists(request, RequestOptions.DEFAULT);
		Assert.assertTrue(exists);
	}

}
