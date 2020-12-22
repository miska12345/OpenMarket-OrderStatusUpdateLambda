package io.openmarket.order.lambda.handler;

import io.openmarket.order.dao.OrderDao;
import io.openmarket.order.model.Order;
import io.openmarket.order.model.OrderStatus;
import io.openmarket.transaction.model.TransactionErrorType;
import io.openmarket.transaction.model.TransactionTaskResult;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

import javax.inject.Inject;
import java.util.Optional;

@Log4j2
public class OrderUpdateLambda {
    private final OrderDao orderDao;

    @Inject
    public OrderUpdateLambda(@NonNull final OrderDao orderDao) {
        this.orderDao = orderDao;
    }

    public void updateOrderStatus(@NonNull final TransactionTaskResult transactionTaskResult) {
        log.info("Processing task {}", transactionTaskResult);
        if (!isTransactionsResultValid(transactionTaskResult)) {
            log.error("Invalid transaction task result: {}", transactionTaskResult);
            return;
        }
        OrderStatus orderStatus = OrderStatus.PAYMENT_NOT_RECEIVED;
        if (transactionTaskResult.getError().equals(TransactionErrorType.NONE)) {
            switch (transactionTaskResult.getStatus()) {
                case COMPLETED:
                    orderStatus = OrderStatus.PAYMENT_CONFIRMED;
                    break;
                default:
                    orderStatus = OrderStatus.PENDING_PAYMENT;
            }
        }
        final Optional<String> optionalOrderId = orderDao.getOrderIdByTransactionId(transactionTaskResult.getTransactionId());
        if (!optionalOrderId.isPresent()) {
            log.info("No orderId is associated with transactionId {}", transactionTaskResult.getTransactionId());
            return;
        }
        final Optional<Order> optionalOrder = orderDao.load(optionalOrderId.get());
        if (!optionalOrder.isPresent()) {
            log.error("No order is associated with orderId {}", optionalOrderId.get());
            return;
        }
        final Order order = optionalOrder.get();
        if (!order.getStatus().equals(OrderStatus.PENDING_PAYMENT)) {
            log.error("State Violation: expected order state for orderId {} to be {} but found {}",
                    order.getOrderId(), OrderStatus.PENDING_PAYMENT, order.getStatus());
            return;
        }
        order.setStatus(orderStatus);
        orderDao.save(order);
        log.info("Updated order status for orderId {} to {}", order.getOrderId(), orderStatus);
    }

    private static boolean isTransactionsResultValid(final TransactionTaskResult transactionTaskResult) {
        return !transactionTaskResult.getTransactionId().isEmpty();
    }
}
