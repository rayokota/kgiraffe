package io.kgraph.kgiraffe.server;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.RxHelper;
import io.vertx.rxjava3.core.Vertx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
@DisplayName("Test the RxJava 3 support")
class FooTest {

    @Test
    @DisplayName("Check the injection of a /io.vertx.reactivex.core.Vertx/ instance")
    void check_injection(Vertx vertx, VertxTestContext testContext) {
        testContext.verify(() -> {
            assertThat(vertx).isNotNull();
            testContext.completeNow();
        });
    }

    @Test
    @DisplayName("Check the deployment and interaction of a Rx verticle")
    void check_deployment_and_message_send(Vertx vertx, VertxTestContext testContext) {
        RxHelper
            .deployVerticle(vertx, new RxVerticle())
            .flatMap(id -> vertx.eventBus().rxRequest("check", "Ok?"))
            .subscribe(
                message -> testContext.verify(() -> {
                    assertThat(message.body()).isEqualTo("Check!");
                    testContext.completeNow();
                }),
                testContext::failNow);
    }

    static class RxVerticle extends AbstractVerticle {

        @Override
        public void start() throws Exception {
            vertx.eventBus().consumer("check", message -> message.reply("Check!"));
        }
    }
}
