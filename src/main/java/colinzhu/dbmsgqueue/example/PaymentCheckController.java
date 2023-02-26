package colinzhu.dbmsgqueue.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class PaymentCheckController extends AbstractVerticle {
    public static void main(String[] args) {
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        Vertx.vertx().deployVerticle(PaymentCheckController.class.getName());
    }

    private int batchId = 0;
    private WebClient client;
    private PaymentRepo paymentRepo;

    @Override
    public void start() throws Exception {
        super.start();
        client = WebClient.create(vertx);
        paymentRepo = new PaymentRepo(vertx);
//        vertx.exceptionHandler(e -> {
//            log.error("Unexpected error, retry in 60s", e);
//            vertx.setTimer(1000*5, id -> getBatchAndProcess());
//        });
        getBatchAndProcess();
    }

    private void getBatchAndProcess() {
        batchId++;
        paymentRepo.findByStatusOrderByCreateTime("CREATED", 100).onSuccess(rows -> {
            if (rows.size() > 0) {
                log.info("Retrieved batchId:{} size: {}", batchId, rows.size());
                List<Payment> payments = new ArrayList<>();
                rows.forEach(row -> payments.add(new Payment(row.getLong("ID"), row.getString("STATUS"), row.getLong("CREATE_TIME"))));

                List<Future> futures = payments.stream().map(this::processSingle).collect(Collectors.toList());
                CompositeFuture.all(futures).onSuccess(event -> {
                    log.info("batchId:{} size: {}, all items processed", batchId, futures.size());
                    getBatchAndProcess();
                }).onFailure(e -> {
                    log.error("Error processing batch, retry in 5000ms", e);
                    vertx.setTimer(5000, id -> getBatchAndProcess());
                });
            } else {
                log.info("batchId:{} size: 0, wait for 5000ms and then continue", batchId);
                vertx.setTimer(5000, id -> getBatchAndProcess());
            }
        }).onFailure(e -> {
            log.error("Error get batch from DB, retry in 60s", e);
            vertx.setTimer(1000*60, id -> getBatchAndProcess());
        });
    }

    private Future<Integer> processSingle(Payment payment) {
        return Future.future(promise -> client.get(8888, "localhost", "/?id=" + payment.getId())
                .send()
                .onSuccess(getHttpResponseHandler(payment, promise))
                .onFailure(err -> {
                    log.info("failed to call check API, retry in 5000ms, {} {}", payment.getId(), err.getMessage());
                    vertx.setTimer(5000, id -> processSingle(payment).onSuccess(promise::complete));
                }));
    }

    private Handler<HttpResponse<Buffer>> getHttpResponseHandler(Payment payment, Promise<Integer> promise) {
        return response -> {
            int statusCode = response.statusCode();
            if (statusCode == 200) {
                paymentRepo.updateStatusById(vertx, promise, "CHECKED", payment.getId());
            } else if (statusCode == 400) {
                paymentRepo.updateStatusById(vertx, promise, "CREATED_CHECK_400", payment.getId());
            } else if (statusCode >= 500 && statusCode <=599) {
                log.info("{},{}, retry in 5000ms", statusCode, payment.getId());
                vertx.setTimer(5000, id -> processSingle(payment).onSuccess(promise::complete));
            }
        };
    }

}
