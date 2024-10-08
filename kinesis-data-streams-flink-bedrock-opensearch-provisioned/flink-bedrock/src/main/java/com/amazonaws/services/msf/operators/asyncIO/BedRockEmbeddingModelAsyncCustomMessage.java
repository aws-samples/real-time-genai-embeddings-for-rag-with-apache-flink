package com.amazonaws.services.msf.operators.asyncIO;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * AsyncFunction implementation for invoking an embedding model using BedrockRuntimeAsyncClient.
 */
public class BedRockEmbeddingModelAsyncCustomMessage extends RichAsyncFunction<JSONObject, JSONObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BedRockEmbeddingModelAsyncCustomMessage.class);

    private transient BedrockRuntimeAsyncClient bedrockClient;
    private String region;
    private String embeddingModel;


    public BedRockEmbeddingModelAsyncCustomMessage(String region, String embeddingModel) {
        this.region = region;
        this.embeddingModel = embeddingModel;
    }

    /**
     * Asynchronously invoke the embedding model and complete the ResultFuture with the result.
     *
     * @throws Exception    If an error occurs during the asynchronous invocation.
     */

    @Override
    public void open(Configuration parameters) throws Exception {
        bedrockClient = BedrockRuntimeAsyncClient.builder()
                .region(Region.of(region))  // Use the specified AWS region
                .build();
    }

    @Override
    public void close() throws Exception {
        bedrockClient.close();
    }


    @Override
    public void asyncInvoke(JSONObject jsonObject, ResultFuture<JSONObject> resultFuture) throws Exception {
        // Simulate an asynchronous call using CompletableFuture
        CompletableFuture.supplyAsync(new Supplier<JSONObject>() {
            @Override
            public JSONObject get() {
                try {
                    // Create BedrockRuntimeAsyncClient for making asynchronous model invocations

                    // Extract text from input JSON object
                    String stringBody = jsonObject.getString("text");
                    ArrayList<String> stringList = new ArrayList<>();

                    stringList.add(stringBody);
                    // Prepare input JSON for the model invocation
                    JSONObject jsonBody = new JSONObject()
                            .put("inputText", stringBody);

                    SdkBytes body = SdkBytes.fromUtf8String(jsonBody.toString());
                    String modelId = new String();

                    if (Objects.equals(embeddingModel, "titan-v1")) {
                        modelId = "amazon.titan-embed-text-v1";
                    }
                    else if (Objects.equals(embeddingModel, "titan-v2")) {
                        modelId = "amazon.titan-embed-text-v2:0";
                    }

                    // Prepare model invocation request
                    InvokeModelRequest request = InvokeModelRequest.builder()
                            .modelId(modelId)
                            .contentType("application/json")
                            .accept("*/*")
                            .body(body)
                            .build();

                    // Invoke the model asynchronously and get the CompletableFuture for the response
                    CompletableFuture<InvokeModelResponse> futureResponse = bedrockClient.invokeModel(request);

                    // Extract and process the response when it is available
                    JSONObject response = new JSONObject(
                            futureResponse.join().body().asString(StandardCharsets.UTF_8)
                    );

                    // Add additional fields to the response
                    response.put("text", jsonObject.get("text"));
                    response.put("@timestamp", jsonObject.get("created_at"));
                    response.put("_id", jsonObject.get("_id"));

                    return response;
                } catch (Exception e) {
                    LOGGER.error("Error during asynchronous invocation", e);
                    return null;
                }
            }
        }).thenAccept((JSONObject result) -> {
            // Complete the ResultFuture with the final result
            resultFuture.complete(Collections.singleton(result));
        });
    }
}
