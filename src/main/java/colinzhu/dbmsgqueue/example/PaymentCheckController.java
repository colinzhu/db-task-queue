package colinzhu.dbmsgqueue.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Slf4j
public class PaymentCheckController extends AbstractVerticle {
    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(PaymentCheckController.class.getName());
    }

    long id = 0;
    int batchId = 0;

    WebClient client;

    PaymentRepo paymentRepo;

    @Override
    public void start() throws Exception {
        super.start();
        client = WebClient.create(vertx);
        paymentRepo = new PaymentRepo(vertx);
        getBatchAndProcess();
    }

    private void getBatchAndProcess() {
        batchId++;
        paymentRepo.findByStatusOrderByCreateTime("CREATED", 100).onSuccess(paymentList -> {
            if (paymentList.size() > 0) {
                log.info("batchId:{} size: {}: {}", batchId, paymentList.size(), paymentList);
                processBatch(paymentList).onSuccess(event -> {
                    log.info("batchId:{} process completed", batchId);
                    getBatchAndProcess();
                });
            } else {
                log.info("batchId:{} size: 0, wait for 5000ms and then continue", batchId);
                vertx.setTimer(5000, id -> {
                    getBatchAndProcess();
                });
            }
        });
    }

    public Future<List<Payment>> getDummyBatch() {
        return Future.future(promise -> {
            List<Payment> paymentList = new ArrayList<>();
            int a = ThreadLocalRandom.current().nextInt(0, 3);
            for (int i = 0; i < 2 * a; i++) {
                id = id + 1;
                paymentList.add(new Payment(id, "CREATED", System.currentTimeMillis()));
            }
            promise.complete(paymentList);
        });
    }

    private Future<Void> processBatch(List<Payment> paymentBatch) {
        return Future.future(promise -> {
            List<Future> futures = paymentBatch.stream().map(this::processSingle).collect(Collectors.toList());
            log.info("Futures size:{}, futures: {}", futures.size(), futures);
            CompositeFuture.all(futures).onSuccess(event -> {
                log.info("batchId:{} process completed", batchId);
                //event.list().stream().forEach(p -> log.info(p.toString()));
                promise.complete();
            });
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
                            long start = System.currentTimeMillis();
                            //payment.setStatus("CHECKED"); // move to another queue
                            paymentRepo.updateStatusById("CHECKED", payment.getId()).onSuccess(event -> {
                                log.info("check completed status:{} payment:{}, {}ms", status, payment.getId(), System.currentTimeMillis()-start);
                                promise.complete(payment);
                            });
                        } else if (status == 400) {
                            long start = System.currentTimeMillis();
                            //payment.setStatus("CREATED_DL");
                            paymentRepo.updateStatusById("CREATED_DL", payment.getId()).onSuccess(event -> {
                                log.info("check stopped status:{} payment:{}, {}ms", status, payment.getId(), System.currentTimeMillis()-start);
                                promise.complete(payment);
                            });
                        } else if (status == 500) {
                            log.info("check retry in 5000ms status:{} payment:{}", status, payment.getId());
                            vertx.setTimer(5000, id -> {
                                processSingle(payment).onSuccess(p -> promise.complete(p));
                            });
                        }
                    })
                    .onFailure(err -> {
                        log.info("payment: {}, retry in 5000ms, something went wrong {}", payment.getId(), err.getMessage());
                        vertx.setTimer(5000, id -> {
                            processSingle(payment).onSuccess(p -> promise.complete(p));
                        });
                    });
        });
    }


}
