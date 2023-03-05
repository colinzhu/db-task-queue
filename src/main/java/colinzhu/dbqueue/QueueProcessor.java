package colinzhu.dbqueue;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class QueueProcessor {
    private final Vertx vertx;
    private final String queueName;
    private int batchId = 0;

    @Setter
    private int noTaskPollInterval = 5000;
    @Setter
    private int processErrRetryInterval = 5000;
    @Setter
    private int errPollingRetryInterval = 60 * 1000;
    @Setter
    private boolean continueWhenNoTask = true;


    public <T> void fetchBatchAndProcess(Supplier<Future<List<T>>> listFutureSupplier, Function<T, Future<?>> itemProcessor, Function<CompositeFuture, Future<?>> postBatchProcessor) {
        batchId++;
        Consumer<Integer> retry = delay -> vertx.setTimer(5000, id -> fetchBatchAndProcess(listFutureSupplier, itemProcessor, postBatchProcessor));
        listFutureSupplier.get()
                .onSuccess(list -> {
                    if (list.size() > 0) {
                        log.info("[{}][Batch:{}] size:{}", queueName, batchId, list.size());
                        List<Future> futures = list.stream().map(itemProcessor).collect(Collectors.toList());
                        CompositeFuture.all(futures).compose(compositeFuture -> {
                            if (postBatchProcessor == null) {
                                return Future.succeededFuture();
                            } else {
                                log.info("[{}][Batch:{}] size:{} continue to call post batch processor", queueName, batchId, list.size());
                                return postBatchProcessor.apply(compositeFuture);
                            }
                        }).onSuccess(event -> {
                            log.info("[{}][Batch:{}] size:{}, all processed", queueName, batchId, futures.size());
                            fetchBatchAndProcess(listFutureSupplier, itemProcessor, postBatchProcessor);
                        }).onFailure(e -> {
                            log.error("[{}][Batch:{}] error processing batch, retry in {}ms", queueName, batchId, processErrRetryInterval, e);
                            retry.accept(5000);
                        });
                    } else {
                        if (continueWhenNoTask) {
                            log.debug("[{}][Batch:{}] size:0, fetch again in {}ms", queueName, batchId, noTaskPollInterval);
                            retry.accept(5000);
                        } else {
                            log.info("[{}][Batch:{}] size:0, no more fetching.", queueName, batchId);
                        }
                    }
                }).onFailure(e -> {
                    log.error("[{}][Batch:{}] Failed to fetch records, retry in {}ms", queueName, errPollingRetryInterval, e);
                    retry.accept(1000 * 60);
                });
    }

}
