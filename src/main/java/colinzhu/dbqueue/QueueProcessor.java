package colinzhu.dbqueue;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Consumer;
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
    private Supplier<Future<List<T>>> listFutureSupplier;
    @Setter
    private Function<T, Future<?>> itemProcessor;
    @Setter
    private Function<CompositeFuture, Future<?>> postBatchProcessor;


    public void fetchBatchAndProcess() {
        long batchId = System.currentTimeMillis();
        listFutureSupplier.get()
                .onSuccess(list -> {
                    if (list.size() > 0) {
                        log.info("[{}][Batch:{}] size:{}, started", queueName, batchId, list.size());
                        List<Future> futures = list.stream().map(itemProcessor).collect(Collectors.toList());
                        CompositeFuture.join(futures).compose(compositeFuture -> {
                            if (postBatchProcessor == null) {
                                return Future.succeededFuture();
                            } else {
                                log.info("[{}][Batch:{}] size:{} continue to call post batch processor", queueName, batchId, list.size());
                                return postBatchProcessor.apply(compositeFuture);
                            }
                        }).onSuccess(event -> {
                            log.info("[{}][Batch:{}] size:{}, all items succeeded", queueName, batchId, futures.size());
                            rerunWithDelay(hasTaskPollInterval);
                        }).onFailure(e -> {
                            // the item processor should handle all the exceptions, this is only a safety net e.g. not update to update record status in DB
                            log.error("[{}][Batch:{}] all items completed, but at least one item failed. Retry in {}ms", queueName, batchId, processErrRetryInterval, e);
                            rerunWithDelay(processErrRetryInterval);
                        });
                    } else {
                        if (continueWhenNoTask) {
                            log.debug("[{}][Batch:{}] size:0, fetch again in {}ms", queueName, batchId, noTaskPollInterval);
                            rerunWithDelay(noTaskPollInterval);
                        } else {
                            log.info("[{}][Batch:{}] size:0, no more fetching.", queueName, batchId);
                        }
                    }
                }).onFailure(e -> {
                    log.error("[{}][Batch:{}] Failed to fetch records, retry in {}ms", queueName, errPollingRetryInterval, e);
                    rerunWithDelay(errPollingRetryInterval);
                });
    }

    private void rerunWithDelay(long delay) {
        vertx.setTimer(delay, id -> fetchBatchAndProcess());
    }
}
