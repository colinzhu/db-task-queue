package colinzhu.dbqueue.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import colinzhu.dbqueue.QueueProcessor;
import colinzhu.dbqueue.RetryApiInvoker;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

@Slf4j
public class PaymentCheckController extends AbstractVerticle {
    public static void main(String[] args) {
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        Logger dbPool = (Logger) LoggerFactory.getLogger("com.mchange.v2.resourcepool.BasicResourcePool");
        dbPool.setLevel(Level.DEBUG);
        Vertx.vertx().deployVerticle(PaymentCheckController.class.getName());
    }
    private WebClient webClient;
    private PaymentRepo paymentRepo;
    private RetryApiInvoker retryApiInvoker;
    private int batchSize = 100;
    private int webClientPoolSize = 1000;

    @Override
    public void start() throws Exception {
        super.start();
        webClient = WebClient.create(vertx, new WebClientOptions().setMaxPoolSize(webClientPoolSize));
        paymentRepo = new PaymentRepo(vertx);
        retryApiInvoker = new RetryApiInvoker(vertx, "API-MSG-CHECK");
        retryApiInvoker.setErr5xxRetryInterval(5000);
        processQueue("CREATED", true);
        log.info("Verticle started.");
    }

    public void processDeadQueue() {
        processQueue("CREATED_DEAD", false);
    }

    private void processQueue(String status, boolean continueWhenNoTask) {
        QueueProcessor<Payment> queueProcessor = new QueueProcessor<>(vertx, "QUEUE-MSG-" + status);
        queueProcessor.noTaskPollInterval(5000)
                .continueWhenNoTask(continueWhenNoTask)
                .batchSupplier(() -> paymentRepo.findByStatusOrderByCreateTime(status, batchSize))
                .itemConsumer(this::processSinglePayment)
                .fetchBatchAndProcess();
    }

    private Future<Integer> processSinglePayment(Payment payment) {
        HttpRequest<Buffer> httpRequest = webClient.get(8888, "localhost", "/?id=" + payment.getId());
        return retryApiInvoker.invokeApi(payment.getId(), httpRequest::send)
                .map(resp -> resp.statusCode() == 200 ? "CHECKED" : "CHECK_ERR_" + resp.statusCode())
                .compose(newStatus -> paymentRepo.updateStatus(payment, newStatus))
                .recover(err -> { // Safety net, in case any unexpected error, update record status to "CHECK_ERR_UNEXP"
                    log.error("[{}] Unexpected error occurred.", payment.getId(), err);
                    return paymentRepo.updateStatus(payment, "CHECK_ERR_UNEXP");
                });
    }
}
