package io.openmarket.order.lambda.handler;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.ImmutableList;
import io.openmarket.order.dao.OrderDao;
import io.openmarket.order.dao.OrderDaoImpl;
import io.openmarket.order.model.ItemInfo;
import io.openmarket.order.model.Order;
import io.openmarket.order.model.OrderStatus;
import io.openmarket.transaction.model.TransactionErrorType;
import io.openmarket.transaction.model.TransactionStatus;
import io.openmarket.transaction.model.TransactionTaskResult;
import io.openmarket.transaction.model.TransactionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.openmarket.config.OrderConfig.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OrderUpdateLambdaTest {
    private static final String BUYER_ID = "tom";
    private static final String SELLER_ID = "jerry";
    private static final String CURRENCY = "DashCoin";
    private static final String ORDER_ID = "a1372131";
    private static final String TRANSACTION_ID = "123";

    private static AmazonDynamoDBLocal localDBClient;
    private AmazonDynamoDB dbClient;
    private DynamoDBMapper dbMapper;
    private OrderDao orderDao;
    private OrderUpdateLambda lambda;

    @BeforeAll
    public static void setupLocalDB() {
        localDBClient = DynamoDBEmbedded.create();
    }

    @BeforeEach
    public void setup() {
        dbClient = localDBClient.amazonDynamoDB();
        dbMapper = new DynamoDBMapper(dbClient);
        orderDao = new OrderDaoImpl(dbClient, dbMapper);
        lambda = new OrderUpdateLambda(orderDao);
        createTable();
    }

    @AfterEach
    public void reset() {
        dbClient.deleteTable(ORDER_DDB_TABLE_NAME);
    }

    @AfterAll
    public static void tearDown() {
        localDBClient.shutdown();
    }

    @Test
    public void test_Update_Order_Status_Normal() {
        TransactionTaskResult taskResult = generateTaskResult(TRANSACTION_ID, TransactionErrorType.NONE,
                TransactionStatus.COMPLETED);
        orderDao.save(generateOrder(ORDER_ID, OrderStatus.PENDING_PAYMENT, TRANSACTION_ID));
        lambda.updateOrderStatus(taskResult);

        Order updatedOrder = orderDao.load(ORDER_ID).get();
        assertEquals(OrderStatus.PAYMENT_CONFIRMED, updatedOrder.getStatus());
    }

    @Test
    public void test_Updated_Order_Status_Insufficient_Balance() {
        TransactionTaskResult taskResult = generateTaskResult(TRANSACTION_ID, TransactionErrorType.INSUFFICIENT_BALANCE,
                TransactionStatus.ERROR);
        orderDao.save(generateOrder(ORDER_ID, OrderStatus.PENDING_PAYMENT, TRANSACTION_ID));
        lambda.updateOrderStatus(taskResult);

        Order updatedOrder = orderDao.load(ORDER_ID).get();
        assertEquals(OrderStatus.PAYMENT_NOT_RECEIVED, updatedOrder.getStatus());
    }

    @Test
    public void test_Do_Nothing_If_Transaction_Is_Not_Order() {
        TransactionTaskResult taskResult = generateTaskResult(TRANSACTION_ID, TransactionErrorType.NONE,
                TransactionStatus.COMPLETED);
        assertDoesNotThrow(() -> lambda.updateOrderStatus(taskResult));
    }

    @ParameterizedTest
    @MethodSource("getArgsForConcurrentUpdateTest")
    public void test_Concurrent_Update_Is_Valid(int numComplete, int numFailed) {
        List<TransactionTaskResult> taskResults = new ArrayList<>();
        for (int i = 0; i < numComplete + numFailed; i++) {
            orderDao.save(generateOrder(String.valueOf(i), OrderStatus.PENDING_PAYMENT, String.valueOf(i)));
            taskResults.add(generateTaskResult(String.valueOf(i),
                    i < numComplete ? TransactionErrorType.NONE : TransactionErrorType.INSUFFICIENT_BALANCE,
                    i < numComplete ? TransactionStatus.COMPLETED : TransactionStatus.ERROR));
        }
        updateConcurrently(taskResults, 2);
        for (int i = 0; i < numComplete + numFailed; i++) {
            if (i < numComplete) {
                assertEquals(OrderStatus.PAYMENT_CONFIRMED, orderDao.load(String.valueOf(i)).get().getStatus());
            } else {
                assertEquals(OrderStatus.PAYMENT_NOT_RECEIVED, orderDao.load(String.valueOf(i)).get().getStatus());
            }
        }
    }

    private static Stream<Arguments> getArgsForConcurrentUpdateTest() {
        return Stream.of(
                Arguments.of(5, 5),
                Arguments.of(5, 0),
                Arguments.of(0, 5)
        );
    }

    private void updateConcurrently(List<TransactionTaskResult> taskResults, int numThreads) {
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        for (TransactionTaskResult res : taskResults) {
            executorService.submit(() -> lambda.updateOrderStatus(res));
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    private static TransactionTaskResult generateTaskResult(String transactionId, TransactionErrorType error,
                                                            TransactionStatus status) {
        return TransactionTaskResult.builder()
                .transactionId(transactionId)
                .status(status)
                .error(error)
                .type(TransactionType.PAY)
                .build();
    }

    private static Order generateOrder(String orderId, OrderStatus status, String transactionId) {
        return Order.builder()
                .orderId(orderId)
                .total(10.0)
                .status(status)
                .sellerId(SELLER_ID)
                .buyerId(BUYER_ID)
                .transactionId(transactionId)
                .currency(CURRENCY)
                .items(ImmutableList.of(ItemInfo.builder()
                        .itemId("a13")
                        .itemName("Cup")
                        .price(5.0)
                        .quantity(2)
                        .build())
                )
                .build();
    }

    private static void createTable() {
        ProvisionedThroughput throughput = new ProvisionedThroughput(5L, 5L);
        localDBClient.amazonDynamoDB().createTable(new CreateTableRequest().withTableName(ORDER_DDB_TABLE_NAME)
                .withKeySchema(ImmutableList.of(new KeySchemaElement(ORDER_DDB_ATTRIBUTE_ORDER_ID, KeyType.HASH)))
                .withAttributeDefinitions(new AttributeDefinition(ORDER_DDB_ATTRIBUTE_ORDER_ID, ScalarAttributeType.S),
                        new AttributeDefinition(ORDER_DDB_ATTRIBUTE_TRANSACTION_ID, ScalarAttributeType.S)
                )
                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                                .withIndexName(ORDER_DDB_INDEX_TRANSACTION_ID_TO_ORDER_ID)
                                .withKeySchema(new KeySchemaElement()
                                        .withAttributeName(ORDER_DDB_ATTRIBUTE_TRANSACTION_ID)
                                        .withKeyType(KeyType.HASH))
                                .withProjection(new Projection().withProjectionType(ProjectionType.INCLUDE)
                                        .withNonKeyAttributes(ORDER_DDB_ATTRIBUTE_ORDER_ID))
                                .withProvisionedThroughput(throughput)
                )
                .withProvisionedThroughput(throughput));
    }
}
