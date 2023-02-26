package colinzhu.dbmsgqueue.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
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
                    log.info("batchId:{} all items processed", batchId);
                    getBatchAndProcess();
                }).onFailure(e -> log.error("Error processing batch", e));
            } else {
                log.info("batchId:{} size: 0, wait for 5000ms and then continue", batchId);
                vertx.setTimer(5000, id -> getBatchAndProcess());
            }
        });
    }

    private Future<Payment> processSingle(Payment payment) {
        return Future.future(promise -> {
            log.info("check started payment:{}", payment.getId());
            client.get(8888, "localhost", "/?id=" + payment.getId())
                    .send()
                    .onSuccess(response -> {
                        int status = response.statusCode();
                        if (status == 200) {
                            paymentRepo.updateStatusById("CHECKED", payment.getId())
                                    .onSuccess(event -> promise.complete(payment));
                        } else if (status == 400) {
                            paymentRepo.updateStatusById("CREATED_DEAD", payment.getId())
                                    .onSuccess(event -> promise.complete(payment));
                        } else if (status == 500) {
                            log.info("check retry in 5000ms status:{} payment:{}", status, payment.getId());
                            vertx.setTimer(5000, id -> processSingle(payment).onSuccess(promise::complete));
                        }
                    })
                    .onFailure(err -> {
                        log.info("payment: {}, retry in 5000ms, something went wrong {}", payment.getId(), err.getMessage());
                        vertx.setTimer(5000, id -> processSingle(payment).onSuccess(promise::complete));
                    });
        });
    }

}
