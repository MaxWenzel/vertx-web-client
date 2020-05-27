package org.devzone;

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.web.client.*;
import org.apache.commons.lang3.time.StopWatch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class VertxWebClient {

    private static final Logger logger = LoggerFactory.getLogger(VertxWebClient.class);

    public static void main(String[] args) throws InterruptedException {

        VertxWebClient webClient = new VertxWebClient();
        StopWatch watch = new StopWatch();
        watch.start();
        webClient.firePostalCodeRequests();
        watch.stop();
        System.out.println("Time Elapsed: " + watch.getTime(TimeUnit.MILLISECONDS));
    }

    public void firePostalCodeRequests() throws InterruptedException {
        Vertx vertx = Vertx.vertx();

        WebClientOptions options = new WebClientOptions()
                .setConnectTimeout(1000)
                .setMaxPoolSize(512)
                .setHttp2MaxPoolSize(512)
                .setTrustAll(true);

        WebClient client = WebClient.create(vertx, options);

        OpenOptions openOptions = new OpenOptions();
        AsyncFile asyncFile = vertx.fileSystem().openBlocking("postalcode_locality_de.csv", openOptions);

        CountDownLatch countDownLatch = new CountDownLatch(12935);

        RecordParser recordParser = RecordParser.newDelimited("\n", bufferedLine -> {
            //System.out.println("bufferedLine = " + bufferedLine);
            String[] parts = bufferedLine.toString().split(",");
            String postalCode = parts[2];
            HttpRequest<Buffer> request = client.get(8082, "localhost", "/postalcodes/" + postalCode);
            MultiMap headers = request.headers();
            headers.set("content-type", "application/json");

            request
                    .send(ar -> {
                        if (ar.succeeded()) {
                            // Obtain response
                            HttpResponse<Buffer> response = ar.result();
                            if (response.statusCode() == 200 && response.getHeader("content-type").contains("application/json")) {
                                //logger.info("Success");
                            } else {
                                logger.error("Something went wrong {}", response.statusCode());
                            }
                        } else {
                            logger.error("Something went wrong {}", ar.cause().getMessage());
                        }
                        countDownLatch.countDown();
                    });
        });

        asyncFile.handler(recordParser)
                .endHandler(v -> {
                    asyncFile.close();
                    logger.info("Done");
                });

        countDownLatch.await();
        logger.info("Finished");
    }


}
