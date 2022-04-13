package io.kgraph.kgiraffe.server.utils;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;

import java.util.concurrent.CompletableFuture;

public class ChainRequestHelper {

    // From https://gist.github.com/purplefox/b7a6ca5fc719a6e59580e18d56af1865
    public static CompletableFuture<HttpResponse<JsonObject>> requestWithFuture(WebClient client,
                                                                                String url,
                                                                                JsonObject json) {
        CompletableFuture<HttpResponse<JsonObject>> cf = new CompletableFuture<>();
        HttpRequest<JsonObject> request = client.post(url)
            .expect(ResponsePredicate.SC_OK)
            .expect(ResponsePredicate.JSON)
            .as(BodyCodec.jsonObject());
        request.sendJsonObject(json, ar -> {
            if (ar.succeeded()) {
                cf.complete(ar.result());
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        return cf;
    }
}
