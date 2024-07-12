/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.msf;


import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.msf.operators.asyncIO.BedRockEmbeddingModelAsyncCustomMessage;

import com.amazonaws.services.msf.operators.map.*;

import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.opensearch.sink.OpensearchSinkBuilder;
import org.apache.flink.connector.opensearch.sink.RestClientFactory;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.json.JSONObject;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Requests;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.regions.Region;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_POSITION;

/**
 * Main class for the Twitter GenAi application that processes and analyzes tweets using Flink and OpenSearch.
 */
public class DataStreamJob {

	// Default configuration values
	//private static final String DEFAULT_OS_DOMAIN = "search-aos-twitter-rag-vector-db-j5ohwaxoo43cegbliwembl3j6q.us-east-1.es.amazonaws.com";

	private static final String DEFAULT_OS_DOMAIN = "";

	private static final String DEFAULT_OS_TWITTER_CUSTOM_INDEX = "";
	private static final String DEFAULT_SOURCE_STREAM = "";
	private static final String DEFAULT_AWS_REGION = "";

	private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamJob.class);

	/**
	 * Checks if the execution environment is local.
	 *
	 * @param env The execution environment.
	 * @return True if the environment is local; false otherwise.
	 */
	private static boolean isLocal(StreamExecutionEnvironment env) {
		return env instanceof LocalStreamEnvironment;
	}

	/**
	 * Loads application parameters from the command line or runtime properties.
	 *
	 * @param args Command line arguments.
	 * @param env  The execution environment.
	 * @return Application parameters.
	 * @throws IOException If an I/O error occurs.
	 */
	private static ParameterTool loadApplicationParameters(String[] args, StreamExecutionEnvironment env) throws IOException {
		if (env instanceof LocalStreamEnvironment) {
			return ParameterTool.fromArgs(args);
		} else {
			Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
			Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");
			if (flinkProperties == null) {
				throw new RuntimeException("Unable to load FlinkApplicationProperties properties from runtime properties");
			}
			Map<String, String> map = new HashMap<>(flinkProperties.size());
			flinkProperties.forEach((k, v) -> map.put((String) k, (String) v));
			return ParameterTool.fromMap(map);
		}
	}

	/**
	 * The main entry point for the application.
	 *
	 * @param args Command line arguments.
	 * @throws Exception If an exception occurs during execution.
	 */
	public static void main(String[] args) throws Exception {

		// Get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Load application parameters
		final ParameterTool applicationProperties = loadApplicationParameters(args, env);


		// OpenSearch configuration values
		String osEndpoint = applicationProperties.get("os.domain", DEFAULT_OS_DOMAIN);
		String embeddingModel = applicationProperties.get("embedding.model");
		String osHost = osEndpoint.replace("https://", "");
		String customMessageIndex = applicationProperties.get("os.custom.index", DEFAULT_OS_TWITTER_CUSTOM_INDEX);
		String region = applicationProperties.get("region", DEFAULT_AWS_REGION);

		// Kinesis Consumer configuration
		Properties kinesisConsumerConfig = new Properties();
		kinesisConsumerConfig.put(AWSConfigConstants.AWS_REGION, region);
		kinesisConsumerConfig.put(STREAM_INITIAL_POSITION, "LATEST");

		// Source for custom stream
		FlinkKinesisConsumer<String> sourceCustom = new FlinkKinesisConsumer<>(
				applicationProperties.get("kinesis.source.stream", DEFAULT_SOURCE_STREAM),
				new SimpleStringSchema(),
				kinesisConsumerConfig
		);

		// Create DataStream of CustomMessage from custom stream
		DataStream<String> input = env.addSource(sourceCustom).uid("source-custom");

		// Convert CustomMessage to JSON and perform asynchronous embedding
		DataStream<JSONObject> customMessageStream = input
				.map(new CustomMessageMapFunction()).uid("custom-message-map-function")
				.map(new CustomMessageToJSONObject()).uid("custom-message-jsonobject-map-function")
				.filter(x -> !x.getString("text").isEmpty());

		DataStream<JSONObject> customMessageEmbedded = AsyncDataStream.unorderedWait(
				customMessageStream,
				new BedRockEmbeddingModelAsyncCustomMessage(region,embeddingModel),
				15000,
				TimeUnit.SECONDS,
				1000
		).uid("custom-message-bedrock-async-function");


// Define the RestClientFactory
		RestClientFactory restClientFactory = new RestClientFactory() {
			@Override
			public void configureRestClientBuilder(RestClientBuilder restClientBuilder, RestClientConfig restClientConfig) {
				HttpRequestInterceptor interceptor = new AwsRequestSigningApacheInterceptor(
						"es",
						Aws4Signer.create(),
						DefaultCredentialsProvider.create(),
						Region.of(region));
				restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.addInterceptorLast(interceptor));

				// Set up the HTTP client with the AWS signer interceptor
			}
		};

		customMessageEmbedded.sinkTo(new OpensearchSinkBuilder<JSONObject>()
				.setBulkFlushInterval(1)
				.setHosts(new HttpHost(osHost,443,"https"))
				.setEmitter((element, context, indexer) -> indexer.add(createIndexRequest2(element,customMessageIndex)))
				.setRestClientFactory(restClientFactory)
				.build());
		// Execute the Flink application

		env.execute("X Flink Bedrock Application");
	}

	public static IndexRequest createIndexRequest2(JSONObject element, String index) {
		Map<String, Object> json = new HashMap<>();
		json.put("passage_embedding", element.getJSONArray("embedding"));
		json.put("date", element.get("@timestamp"));
		json.put("text", element.get("text"));
		// Create and return an IndexRequest
		return Requests.indexRequest()
				.index(index)
				//.id(element.get("_id").toString())
				.source(json, XContentType.JSON);
	}

}
