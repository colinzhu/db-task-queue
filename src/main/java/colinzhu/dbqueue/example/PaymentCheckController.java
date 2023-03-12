package colinzhu.dbqueue.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import colinzhu.dbqueue.QueueProcessor;
import colinzhu.dbqueue.RetryApiInvoker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxInfluxDbOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

@Slf4j
public class PaymentCheckController extends AbstractVerticle {
    MeterRegistry registry = BackendRegistries.getDefaultNow();
    Timer timer = Timer
            .builder("my.timer")
            .description("Time tracker for my extremely sophisticated algorithm")
            .register(registry);

    Counter counter1 = Counter.builder("my.counter").tags("app","db-message-queue","type","abc","result","200").register(registry);
    Counter counter2 = Counter.builder("my.counter").tags("app","db-message-queue","type","abc","result","400").register(registry);

    public static void main(String[] args) {
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        Logger dbPool = (Logger) LoggerFactory.getLogger("com.mchange.v2.resourcepool.BasicResourcePool");
        dbPool.setLevel(Level.DEBUG);
        Vertx vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setInfluxDbOptions(new VertxInfluxDbOptions()
                                .setEnabled(true)
                                .setOrg("colin-org")
                                .setBucket("colin-bucket")
                                .setToken("XILioNsZUEnrSM0A-hOz1hMAZM5ausAQGkLe1qkyeweMLVhP4YZcD9ZWtWq81hwPVCvAcczO_WopMdGrIZx7Pw=="))
                        .setEnabled(true)));
        vertx.deployVerticle(PaymentCheckController.class.getName());
    }
    private WebClient webClient;
    private PaymentRepo paymentRepo;
    private RetryApiInvoker retryApiInvoker;
    private int batchSize = 1000;
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
        counter1.increment();
        counter2.increment();
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
