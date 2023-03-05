package colinzhu.dbqueue;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Accessors(chain = true)
public class RetryApiInvoker {
    private final Vertx vertx;
    private final String apiName;

    @Setter
    private int serverErrRetryInterval = 30 * 1000;
    @Setter
    private int networkErrRetryInterval = 30 * 1000;

    public void callHttpApiWithRetry(String corrId, Supplier<Future<HttpResponse<Buffer>>> httpRespFutureSupplier, Promise<HttpResponse<Buffer>> promise) {
        Consumer<Integer> retry = delay -> vertx.setTimer(delay, id -> callHttpApiWithRetry(corrId, httpRespFutureSupplier, promise));
        httpRespFutureSupplier.get()
                .onSuccess(httpResp -> {
                    int statusCode = httpResp.statusCode();
                    if (statusCode == 200) {
                        log.info("[{}] [{}] {}", apiName, corrId, statusCode);
                        promise.complete(httpResp);
                    } else if (statusCode == 400) {
                        log.info("[{}] [{}] {}", apiName, corrId, statusCode);
                        promise.complete(httpResp);
                    } else if (statusCode >= 500 && statusCode <= 599) {
                        log.info("[{}] [{}] {} retry in {}ms", apiName, corrId, statusCode, serverErrRetryInterval);
                        retry.accept(serverErrRetryInterval);
                    } else {
                        log.info("[{}] [{}] {} retry in {}ms", apiName, corrId, statusCode, serverErrRetryInterval);
                        retry.accept(serverErrRetryInterval);
                    }
                })
                .onFailure(err -> {
                    log.info("[{}] [{}] Failed to call API, retry in {}ms, {}", apiName, corrId, networkErrRetryInterval, err.getMessage());
                    retry.accept(networkErrRetryInterval);
                });
    }

}
