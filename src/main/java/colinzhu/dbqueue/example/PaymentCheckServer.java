package colinzhu.dbqueue.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;

@Slf4j
public class PaymentCheckServer extends AbstractVerticle {
    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(PaymentCheckServer.class.getName());
    }

    @Override
    public void start(Promise<Void> startPromise) {
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        router.route().handler(routingContext -> {
            Random random = new Random();
            int[] values = {200, 400, 500};
            int randomStatusCode = values[random.nextInt(values.length)];
            log.info("[{}] request received {}",routingContext.request().getParam("id"), randomStatusCode);
            vertx.setTimer(10, id -> {
                routingContext.response().setStatusCode(randomStatusCode).end(randomStatusCode + "");
            });
        });
        server.requestHandler(router).listen(8888);
    }

}
