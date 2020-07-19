package com.huyaoban.elasticsearch.document.api;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestStatus;
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
public class UpdateApiTest {
	@Autowired
	private RestHighLevelClient restHighLevelClient;

	@Test
	public void test1() throws IOException {
		UpdateRequest request = new UpdateRequest("posts", "1");
		String jsonString = "{\"updated\":\"2017-01-01\"," +
			"\"reason\":\"daily update\"}";
		request.doc(jsonString, XContentType.JSON);

		UpdateResponse response = restHighLevelClient.update(request, RequestOptions.DEFAULT);
		printUpdateResponse(response);

	}

	@Test
	public void test2() throws IOException {
		//upsert:更新的文档不存在，就插入一个新文档
		UpdateRequest request = new UpdateRequest("posts", "1");
		String jsonString = "{\"updated\":\"2017-01-01\"," +
				"\"reason\":\"daily update\"}";
		request.upsert(jsonString, XContentType.JSON);

		UpdateResponse response = restHighLevelClient.update(request, RequestOptions.DEFAULT);
		printUpdateResponse(response);

	}

	public void test3() {
		UpdateRequest request = new UpdateRequest("posts", "1");
		//冲突时的重试次数
		request.retryOnConflict(3);

		//取回更新后的文档
		request.fetchSource(true);
	}


	@Test
	public void test4() throws IOException {
		//删除指定版本的文档，不存在时抛异常
		UpdateRequest request2 = new UpdateRequest("posts", "1").setIfSeqNo(100).setIfPrimaryTerm(3);
		try {
			UpdateResponse response2 = restHighLevelClient.update(request2, RequestOptions.DEFAULT);
		} catch (ElasticsearchException e) {
			if(e.status() == RestStatus.CONFLICT) {
				log.error("版本冲突");
			} else if(e.status() == RestStatus.NOT_FOUND) {
				log.error("文档不存在");
			}
		}
	}

	private void printUpdateResponse(UpdateResponse updateResponse) {
		String index = updateResponse.getIndex();
		String id = updateResponse.getId();
		long version = updateResponse.getVersion();

		if(updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
			//调用upsert时，如果文档不存在就插入
		} else if(updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
			//文档更新成功
		} else if(updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
			//更新的文档不存在
		} else if(updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
			//更新不起作用，可能更新前和更新后的文档是一样的？
			//Handle the case where the document was not impacted by the update, ie no operation (noop) was executed on the document
		}

		//处理部分分片更新失败
		ReplicationResponse.ShardInfo shardInfo = updateResponse.getShardInfo();
		if(shardInfo.getTotal() != shardInfo.getSuccessful()) {
			log.info("有部分分片还没有删除成功");
		}
		if(shardInfo.getFailed() > 0) {
			for(ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
				//失败原因
				String reason = failure.reason();
			}
		}

		//如果更新请求开启返回更新后的文档，可获取更新后的文档
		GetResult result = updateResponse.getGetResult();
		if(result.isExists()) {
			String sourceAsString = result.sourceAsString();
			Map<String, Object> sourceAsMap = result.sourceAsMap();
			byte[] sourceAsBytes = result.source();
		} else {
			//不返回更新后的文档
		}
	}

}
