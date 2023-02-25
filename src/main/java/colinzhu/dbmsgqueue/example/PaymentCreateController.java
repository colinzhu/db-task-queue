package colinzhu.dbmsgqueue.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class PaymentCreateController extends AbstractVerticle {
    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(PaymentCreateController.class.getName());
    }

    PaymentRepo paymentRepo;

    @Override
    public void start() throws Exception {
        super.start();
        paymentRepo = new PaymentRepo(vertx);
        createPayment();
    }

    private void createPayment() {
        List<Future> allPaymentFutures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            long start = System.currentTimeMillis();
            long id = System.nanoTime();
            Future<Void> future = paymentRepo.insert(new Payment(id, "CREATED", System.currentTimeMillis())).onSuccess(event -> {
                log.info("inserted {}, {}ms", id, System.currentTimeMillis() - start);
            });
            allPaymentFutures.add(future);
        }
        CompositeFuture.all(allPaymentFutures).onSuccess(event -> {
            log.info("create payments completed");
            System.exit(0);
        });

//        vertx.setTimer(1, id -> {
//            count++;
//            long start = System.currentTimeMillis();
//            paymentRepo.insert(new Payment(paymentStartId + count, "CREATED", System.currentTimeMillis())).onSuccess(event -> {
//                log.info("inserted {}, {}ms", paymentStartId + count, System.currentTimeMillis()-start);
//                createPayment();
//            });
//        });
    }

}
