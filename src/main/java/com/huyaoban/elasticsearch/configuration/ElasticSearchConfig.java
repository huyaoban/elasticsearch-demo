package com.huyaoban.elasticsearch.configuration;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class ElasticSearchConfig {

	@Primary
	@Bean(destroyMethod = "close")
	public RestHighLevelClient restHighLevelClient() {
		return new RestHighLevelClient(RestClient.builder(new HttpHost("10.20.0.64", 9200, "http"),
				new HttpHost("10.20.0.65", 9200, "http"), new HttpHost("10.20.0.66", 9200, "http")));
	}
}
