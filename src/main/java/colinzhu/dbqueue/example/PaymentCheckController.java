package colinzhu.dbqueue.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import colinzhu.dbqueue.RetryApiInvoker;
import colinzhu.dbqueue.QueueProcessor;
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

import java.util.List;
import java.util.stream.Collectors;

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
        retryApiInvoker = new RetryApiInvoker(vertx, "API-MSG-CHECK");
        retryApiInvoker.setServerErrRetryInterval(5000);
        processQueue("CREATED", true);
    }

    public void processDeadQueue() {
        processQueue("CREATED_DEAD", false);
    }

    private void processQueue(String status, boolean continueWhenNoTask) {
        QueueProcessor<Payment> queueProcessor = new QueueProcessor(vertx, "QUEUE-MSG-" + status);
        queueProcessor.noTaskPollInterval(5000)
                .continueWhenNoTask(continueWhenNoTask)
                .listFutureSupplier(() -> paymentRepo.findByStatusOrderByCreateTime(status, 100))
                .itemProcessor(this::processSinglePayment)
                .fetchBatchAndProcess();
        //queueProcessor.fetchBatchAndProcess(() -> paymentRepo.findByStatusOrderByCreateTime(status, 100), this::processSinglePayment, null);
    }

    private Future<Integer> processSinglePayment(Payment payment) {
        HttpRequest<Buffer> httpRequest = client.get(8888, "localhost", "/?id=" + payment.getId());
        Future<HttpResponse<Buffer>> respFuture = Future.future(promise -> retryApiInvoker.callHttpApiWithRetry(String.valueOf(payment.getId()), httpRequest::send, promise));
        return respFuture.map(httpResp -> {
            String newStatus;
            if (httpResp.statusCode() == 200) {
                newStatus = "CHECKED";
            } else {
                newStatus = "CHECK_ERROR_" + httpResp.statusCode();
            }
            return newStatus;
        }).compose(newStatus -> paymentRepo.updateStatus(payment, newStatus))
                .onFailure(err -> { // Safety net, in case any unexpected error, update record status to "CHECK_ERROR_UNEXPECTED"
                    log.error("[{}] Unexpected error occurred.", payment.getId(), err);
                    paymentRepo.updateStatus(payment, "CHECK_ERROR_UNEXPECTED");
                });
    }

    private Future<Integer> postBatchProcessing(CompositeFuture compositeFuture) {
        List<Payment> payments = compositeFuture.list().stream().map(i -> (Payment) i).collect(Collectors.toList());
        return paymentRepo.updateStatusInBatch(payments);
    }
}
