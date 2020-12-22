package io.openmarket.order.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import io.openmarket.order.dao.OrderDao;
import io.openmarket.order.dao.OrderDaoImpl;
import io.openmarket.order.model.Order;
import io.openmarket.order.model.OrderStatus;
import io.openmarket.transaction.dao.dynamodb.TransactionDao;
import io.openmarket.transaction.dao.dynamodb.TransactionDaoImpl;
import io.openmarket.transaction.dao.sqs.SQSTransactionTaskPublisher;
import io.openmarket.transaction.model.Transaction;
import io.openmarket.transaction.model.TransactionStatus;
import io.openmarket.transaction.model.TransactionTask;
import io.openmarket.transaction.model.TransactionType;

public class CLI {
    public static void main(String[] args) {
        AmazonDynamoDB dbClient = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDBMapper mapper = new DynamoDBMapper(dbClient);
        TransactionDao transactionDao = new TransactionDaoImpl(dbClient, mapper);
        OrderDao orderDao = new OrderDaoImpl(dbClient, mapper);
        String transacId = "asdasd";

        orderDao.save(Order.builder().status(OrderStatus.PENDING_PAYMENT).buyerId("123").sellerId("321").orderId(transacId)
                .currency("DashCoin")
                .transactionId(transacId).total(3.14).build());

        Transaction transaction = Transaction.builder()
                .transactionId(transacId)
                .payerId("123")
                .recipientId("a")
                .currencyId("DashCoin")
                .amount(2.0)
                .status(TransactionStatus.PENDING)
                .type(TransactionType.TRANSFER).build();
        transactionDao.save(transaction);

        SQSTransactionTaskPublisher pub = new SQSTransactionTaskPublisher(AmazonSQSClientBuilder.standard().build());
        pub.publish("https://sqs.us-west-2.amazonaws.com/185046651126/TransactionTaskQueue", new TransactionTask(transacId));
    }
}
