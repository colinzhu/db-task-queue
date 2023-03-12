package colinzhu.dbqueue.example;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.templates.SqlTemplate;
import io.vertx.sqlclient.templates.TupleMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class PaymentRepo {
    /*
    java -cp /home/colin/.m2/repository/com/h2database/h2/2.1.214/h2-2.1.214.jar org.h2.tools.Server -tcp -ifNotExists -baseDir ./example-db
     */

    private final JDBCPool pool;

    private final TupleMapper<Payment> updatePaymentStatusToParamMapper = TupleMapper.mapper(payment -> {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", payment.getId());
        parameters.put("status", payment.getStatus());
        return parameters;
    });

    public PaymentRepo(Vertx vertx) {
        final JsonObject config = new JsonObject()
                .put("url", "jdbc:h2:tcp://127.0.1.1:9092/example-db")
                .put("driver_class", "org.h2.Driver")
                .put("datasourceName", "example-db")
                .put("user", "sa")
                .put("password", "sa")
                .put("max_pool_size", 20);

        pool = JDBCPool.pool(vertx, config);

    }

    public Future<List<Payment>> findByStatusOrderByCreateTime(String status, String instance, int limit) {
        return pool.preparedQuery("SELECT * FROM PAYMENT WHERE STATUS = ? AND INSTANCE = ? order by CREATE_TIME LIMIT ?")
                .execute(Tuple.of(status, instance, limit))
                .compose(rows -> {
                    List<Payment> payments = new ArrayList<>();
                    rows.forEach(row -> payments.add(new Payment(row.getLong("ID"), row.getString("STATUS"), row.getString("INSTANCE"), row.getLong("CREATE_TIME"))));
                    return  Future.succeededFuture(payments);
                });
    }

    public Future<Integer> updateStatus(Payment oriPayment, String newStatus) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forUpdate(pool, "UPDATE PAYMENT SET STATUS=#{newStatus} WHERE id=#{id}")
                .execute(Map.of("newStatus", newStatus, "id", oriPayment.getId()))
                .map(SqlResult::rowCount)
                .onSuccess(ar -> {
                    if (ar == 1) {
                        log.info("[updateStatus] [{}] [{}] completed, time:{}ms", oriPayment.getId(), newStatus, System.currentTimeMillis() - start);
                    } else {
                        log.warn("[updateStatus] [{}] [{}] failed, rowCount:{}, time:{}ms", oriPayment.getId(), newStatus, System.currentTimeMillis() - start, ar);
                    }
                })
                .onFailure(err -> log.error("[updateStatus] [{}] error", oriPayment.getId(), err));
    }

    public Future<RowSet<Row>> insert(Payment payment, int number) {
        long start = System.currentTimeMillis();
        return pool.preparedQuery("insert into PAYMENT (ID, STATUS, INSTANCE, CREATE_TIME) values (?, ?, ?, ?)")
                .execute(Tuple.of(payment.getId(), payment.getStatus(), payment.getInstance(), payment.getCreateTime()))
                .onSuccess(rows -> log.info("#{} inserted, time:{}ms", number, System.currentTimeMillis() - start))
                .onFailure(e -> log.error("error inserting", e));
    }

    public Future<List<Payment>> searchBy(SearchCriteria criteria) {
        return pool.preparedQuery("SELECT * FROM PAYMENT WHERE STATUS = ? order by CREATE_TIME LIMIT ?")
                .execute(Tuple.of(criteria.getStatus(), criteria.getLimit()))
                .compose(rows -> {
                    List<Payment> payments = new ArrayList<>();
                    rows.forEach(row -> payments.add(new Payment(row.getLong("ID"), row.getString("STATUS"), row.getString("INSTANCE"), row.getLong("CREATE_TIME"))));
                    return  Future.succeededFuture(payments);
                });
    }

    @Data
    public static class SearchCriteria {
        private List<Long> idList;
        private String status;
        private LocalDate valueDateFrom;
        private LocalDate valueDateTo;
        private LocalDateTime createTimeFrom;
        private LocalDateTime createTimeTo;
        private String orderBy;
        private Integer limit;
    }


}
