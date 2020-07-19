package com.huyaoban.elasticsearch.document.api;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class DeleteApiTest {
	@Autowired
	private RestHighLevelClient restHighLevelClient;

	@Test
	public void test1() throws IOException {
		DeleteRequest request = new DeleteRequest("posts", "1");
		DeleteResponse response = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
		printDeleteResponse(response);

	}


	@Test
	public void test4() throws IOException {
		//删除指定版本的文档，不存在时抛异常
		DeleteRequest request2 = new DeleteRequest("posts", "1").setIfSeqNo(100).setIfPrimaryTerm(3);
		try {
			DeleteResponse response2 = restHighLevelClient.delete(request2, RequestOptions.DEFAULT);
		} catch (ElasticsearchException e) {
			if(e.status() == RestStatus.CONFLICT) {
				log.error("版本冲突");
			}
		}
	}

	private void printDeleteResponse(DeleteResponse deleteResponse) {
		String index = deleteResponse.getIndex();
		String id = deleteResponse.getId();
		long version = deleteResponse.getVersion();
		ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();

		if(deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
			log.error("删除的文档不存在");
		}

		if(shardInfo.getTotal() != shardInfo.getSuccessful()) {
			log.info("有部分分片还没有删除成功");
		}
		if(shardInfo.getFailed() > 0) {
			for(ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
				//失败原因
				String reason = failure.reason();
			}
		}
	}

}
