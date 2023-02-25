package colinzhu.dbmsgqueue.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PaymentCheckServer extends AbstractVerticle {
    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(PaymentCheckServer.class.getName());
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        router.route().handler(routingContext -> {
            String paramId = routingContext.request().getParam("id");
            log.info("request received " + routingContext.request().getParam("id"));
            vertx.setTimer(10, id -> {
                if (null != paramId && paramId.endsWith("4")) {
                    routingContext.response()
                            .putHeader("content-type", "text/plain")
                            .setStatusCode(400)
                            .end("400 Hello from Vert.x! " + paramId);

                } else if (null != paramId && paramId.endsWith("5A")){
                    routingContext.response()
                            .putHeader("content-type", "text/plain")
                            .setStatusCode(500)
                            .end("500 Hello from Vert.x! " + paramId);
                } else {
                    routingContext.response()
                            .putHeader("content-type", "text/plain")
                            .end("Hello from Vert.x! " + paramId);
                }
            });
        });
        server.requestHandler(router).listen(8888);
    }

}
