package io.openmarket.order.lambda.entry;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.openmarket.config.EnvConfig;
import io.openmarket.marketplace.dao.ItemDaoImpl;
import io.openmarket.order.dao.OrderDao;
import io.openmarket.order.dao.OrderDaoImpl;
import io.openmarket.order.lambda.handler.OrderUpdateLambda;
import io.openmarket.transaction.model.TransactionTaskResult;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

/**
 * Handler for requests to Lambda function.
 */
@Log4j2
public class LambdaEntry implements RequestHandler<SNSEvent, Void> {
    private static final Gson GSON = new Gson();
    public Void handleRequest(@NonNull final SNSEvent input, final Context context) {
        final AmazonDynamoDB dbClient = AmazonDynamoDBClientBuilder.standard().build();
        final OrderDao orderDao = new OrderDaoImpl(dbClient, new DynamoDBMapper(dbClient));
        final SessionFactory sessionFactory = buildSessionFactory();
        final OrderUpdateLambda handler = new OrderUpdateLambda(orderDao, new ItemDaoImpl(sessionFactory));
        final Type listType = new TypeToken<ArrayList<TransactionTaskResult>>(){}.getType();
        int updatedCount = 0;
        int failedCount = 0;
        log.info("Received {} SNS events", input.getRecords().size());
        for (SNSEvent.SNSRecord record : input.getRecords()) {
            final List<TransactionTaskResult> tResult;
            try {
                tResult = GSON.fromJson(record.getSNS().getMessage(), listType);
                log.info("Start processing {} transaction updates", tResult.size());
                for (TransactionTaskResult res : tResult) {
                    try {
                        handler.updateOrderStatus(res);
                        updatedCount++;
                    } catch (Exception e) {
                        log.error("Order associated with transactionId {} has been modified externally",
                                res.getTransactionId(), e);
                        failedCount++;
                    }
                }
            } catch (JsonSyntaxException e) {
                log.error("Message is not recognizable: {}", record.getSNS().getMessage(), e);
                failedCount++;
            }
        }
        log.info("Done processing, updated {}, failed {}", updatedCount, failedCount);
        return null;
    }

    private SessionFactory buildSessionFactory() {
        Configuration cfg = new Configuration().configure();
        cfg.setProperty("hibernate.connection.url", System.getenv(EnvConfig.ENV_VAR_DB_URL));
        cfg.setProperty("hibernate.connection.username", System.getenv(EnvConfig.ENV_VAR_DB_USER));
        cfg.setProperty("hibernate.connection.password", System.getenv(EnvConfig.ENV_VAR_DB_PASSWORD));
        cfg.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQL5Dialect");

        return cfg.buildSessionFactory();
    }
}
