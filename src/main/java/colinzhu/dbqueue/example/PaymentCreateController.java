package colinzhu.dbqueue.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class PaymentCreateController extends AbstractVerticle {
    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(PaymentCreateController.class.getName());
    }

    private PaymentRepo paymentRepo;

    @Override
    public void start() throws Exception {
        super.start();
        paymentRepo = new PaymentRepo(vertx);
        createPayment();
    }

    private void createPayment() {
        long start = System.currentTimeMillis();
        List<Future> allPaymentFutures = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Future<RowSet<Row>> future = paymentRepo.insert(new Payment(System.nanoTime(), "CREATED", System.currentTimeMillis()), i);
            allPaymentFutures.add(future);
        }
        CompositeFuture.all(allPaymentFutures).onSuccess(event -> {
            log.info("create payments completed, time: {}ms", System.currentTimeMillis() - start);
            System.exit(0);
        }).onFailure(e -> log.info("error waiting for all", e));
    }

}
