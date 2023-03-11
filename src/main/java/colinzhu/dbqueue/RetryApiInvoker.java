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
    private int err401RetryInterval = 30 * 1000;
    @Setter
    private int err5xxRetryInterval = 30 * 1000;
    @Setter
    private int networkErrRetryInterval = 30 * 1000;

    /**
     * The method will only return Future of status code 200 or 400, for other cases it will retry until get 200 or 400
     * @param corrId for logging
     * @param httpRespFutureSupplier the action to fire the http request and get the http response
     * @return
     */
    public Future<HttpResponse<Buffer>> invokeApi(Object corrId, Supplier<Future<HttpResponse<Buffer>>> httpRespFutureSupplier) {
        return Future.future(promise -> callHttpApiWithRetry(String.valueOf(corrId), httpRespFutureSupplier, promise));
    }

    private void callHttpApiWithRetry(String corrId, Supplier<Future<HttpResponse<Buffer>>> httpRespFutureSupplier, Promise<HttpResponse<Buffer>> promise) {
        Consumer<Integer> delayAndRetry = delay -> vertx.setTimer(delay, id -> callHttpApiWithRetry(corrId, httpRespFutureSupplier, promise));
        httpRespFutureSupplier.get()
                .onSuccess(httpResp -> {
                    int statusCode = httpResp.statusCode();
                    if (statusCode >= 200 && statusCode <= 299) {
                        log.info("[{}] [{}] {}", apiName, corrId, statusCode);
                        promise.complete(httpResp);
                    } else if (statusCode == 401) { // pls make sure retry can fix the 401 error. E.g. with new token
                        log.info("[{}] [{}] {} retry in {}ms", apiName, corrId, statusCode, err401RetryInterval);
                        delayAndRetry.accept(err401RetryInterval);
                    } else if (statusCode >= 400 && statusCode <= 499) {
                        log.info("[{}] [{}] {}", apiName, corrId, statusCode);
                        promise.complete(httpResp);
                    } else { // 5xx and 3xx errors
                        log.info("[{}] [{}] {} retry in {}ms", apiName, corrId, statusCode, err5xxRetryInterval);
                        delayAndRetry.accept(err5xxRetryInterval);
                    }
                })
                .onFailure(err -> { // network error
                    log.info("[{}] [{}] Failed to call API, retry in {}ms, {}", apiName, corrId, networkErrRetryInterval, err.getMessage());
                    delayAndRetry.accept(networkErrRetryInterval);
                });
    }

}
