package colinzhu.dbqueue;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Accessors(fluent = true)
public class QueueProcessor<T> {
    private final Vertx vertx;
    private final String queueName;

    @Setter
    private int noTaskPollInterval = 5000;
    @Setter
    private int hasTaskPollInterval = 1;
    @Setter
    private int processErrRetryInterval = 5000;
    @Setter
    private int errPollingRetryInterval = 60 * 1000;
    @Setter
    private boolean continueWhenNoTask = true;
    @Setter
    private Supplier<Future<List<T>>> batchSupplier;
    @Setter
    private Function<T, Future<?>> itemConsumer;

    public void fetchBatchAndProcess() {
        long batchId = System.currentTimeMillis();
        batchSupplier.get().onSuccess(batch -> {
            if (batch.size() > 0) {
                log.info("[{}][Batch:{}] size:{}, fetched. Time:{}ms", queueName, batchId, batch.size(), System.currentTimeMillis() - batchId);
                long procStart = System.currentTimeMillis();
                List<Future> futures = batch.stream().map(itemConsumer).collect(Collectors.toList());
                CompositeFuture.join(futures).onSuccess(event -> {
                    long end = System.currentTimeMillis();
                    log.info("[{}][Batch:{}] size:{}, all items succeeded. Fetch and process time:{}ms, fetch time:{}ms process time:{}ms", queueName, batchId, futures.size(), end - batchId, procStart - batchId, end - procStart);
                    rerunWithDelay(hasTaskPollInterval);
                }).onFailure(e -> {
                    // item consumer should handle all exceptions, this is only a safety net e.g. not able to update record status in DB
                    log.error("[{}][Batch:{}] size:{}, all items completed, but at least one item failed. Retry in {}ms", queueName, batchId, batch.size(), processErrRetryInterval, e);
                    rerunWithDelay(processErrRetryInterval);
                });
            } else {
                if (continueWhenNoTask) {
                    log.debug("[{}][Batch:{}] size:0. Time:{}ms. Fetch again in {}ms", queueName, batchId, System.currentTimeMillis()-batchId, noTaskPollInterval);
                    rerunWithDelay(noTaskPollInterval);
                } else {
                    log.info("[{}][Batch:{}] size:0, no more fetching.", queueName, batchId);
                }
            }
        }).onFailure(e -> {
            log.error("[{}][Batch:{}] Failed to fetch batch, retry in {}ms", queueName, batchId, errPollingRetryInterval, e);
            rerunWithDelay(errPollingRetryInterval);
        });
    }

    private void rerunWithDelay(long delay) {
        vertx.setTimer(delay, id -> fetchBatchAndProcess());
    }
}
