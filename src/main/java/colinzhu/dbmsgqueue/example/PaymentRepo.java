package colinzhu.dbmsgqueue.example;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class PaymentRepo {
    /*
    java -cp /home/colin/.m2/repository/com/h2database/h2/2.1.214/h2-2.1.214.jar org.h2.tools.Server -tcp -ifNotExists -baseDir ./example-db
     */

    private JDBCPool pool;

    public PaymentRepo(Vertx vertx) {
        final JsonObject config = new JsonObject()
                .put("url", "jdbc:h2:tcp://127.0.1.1:9092/example-db")
                .put("driver_class", "org.h2.Driver")
                .put("datasourceName", "example-db")
                .put("user", "sa")
                .put("password", "sa")
                .put("max_pool_size", 16);

        pool = JDBCPool.pool(vertx, config);

    }

    public Future<List<Payment>> findByStatusOrderByCreateTime(String status, int limit) {
        return Future.future(promise -> {
            pool.preparedQuery("SELECT * FROM PAYMENT WHERE STATUS = ? order by CREATE_TIME LIMIT ?")
                    .execute(Tuple.of(status, limit))
                    .onFailure(e -> {
                        log.error("error", e);
                    })
                    .onSuccess(rows -> {
                        List<Payment> paymentList = new ArrayList<>();
                        for (Row row : rows) {
                            paymentList.add(new Payment(row.getLong("ID"), row.getString("STATUS"), row.getLong("CREATE_TIME")));
                            System.out.println(row.getInteger("ID") + " " + row.getString("STATUS") + " " + row.getLong("CREATE_TIME"));
                        }
                        promise.complete(paymentList);
                    });
        });
    }


    public Future<Void> updateStatusById(String status, Long id) {
        return Future.future(promise -> {
            pool.preparedQuery("UPDATE PAYMENT SET STATUS = ? WHERE ID = ?")
                    .execute(Tuple.of(status, id))
                    .onFailure(e -> {
                        log.error("error", e);
                    })
                    .onSuccess(event -> promise.complete());
        });
    }


    public Future<Void> insert(Payment payment) {
        return Future.future(promise -> {
            pool.preparedQuery("insert into PAYMENT (ID, STATUS, CREATE_TIME) values (?, ?, ?)")
                    .execute(Tuple.of(payment.getId(), payment.getStatus(), payment.getCreateTime()))
                    .onFailure(e -> {
                        log.error("error", e);
                    })
                    .onSuccess(rows -> {
                        for (Row row : rows) {
                            System.out.println(row.getInteger("ID") + " " + row.getString("STATUS") + " inserted");
                        }
                        promise.complete();
                    });
        });
    }

}
