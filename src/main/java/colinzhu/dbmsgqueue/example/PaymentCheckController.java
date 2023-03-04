package colinzhu.dbmsgqueue.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import colinzhu.dbmsgqueue.RetryApiInvoker;
import colinzhu.dbmsgqueue.QueueProcessor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

@Slf4j
public class PaymentCheckController extends AbstractVerticle {
    public static void main(String[] args) {
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        Vertx.vertx().deployVerticle(PaymentCheckController.class.getName());
    }
    private WebClient client;
    private PaymentRepo paymentRepo;
    private RetryApiInvoker retryApiInvoker;

    @Override
    public void start() throws Exception {
        super.start();
        client = WebClient.create(vertx);
        paymentRepo = new PaymentRepo(vertx);
        retryApiInvoker = new RetryApiInvoker(vertx, "MSG-CHECK");
        retryApiInvoker.setServerErrRetryInterval(5000);
        QueueProcessor queueProcessor = new QueueProcessor(vertx, "MSG-CREATED");
        queueProcessor.setNoTaskPollInterval(5000);
        queueProcessor.fetchBatchAndProcess(() -> paymentRepo.findByStatusOrderByCreateTime("CREATED", 100), this::processSinglePayment, this::postBatchProcessing);
    }

    private Future<Integer> processSinglePayment(Payment payment) {
        HttpRequest<Buffer> httpRequest = client.get(8888, "localhost", "/?id=" + payment.getId());
        Future<HttpResponse<Buffer>> respFuture = Future.future(promise -> retryApiInvoker.callHttpApiWithRetry(String.valueOf(payment.getId()), httpRequest::send, promise));
        return respFuture.compose(response -> paymentRepo.updateStatusById("CHECKED", payment.getId()));
    }

    private Future<Void> postBatchProcessing(CompositeFuture compositeFuture) {
        compositeFuture.list().forEach(i -> log.info(i.toString()));
        return Future.succeededFuture();
    }
}
