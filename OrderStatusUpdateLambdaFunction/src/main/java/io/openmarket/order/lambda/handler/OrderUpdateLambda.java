package io.openmarket.order.lambda.handler;

import io.openmarket.marketplace.dao.ItemDao;
import io.openmarket.order.dao.OrderDao;
import io.openmarket.order.model.ItemInfo;
import io.openmarket.order.model.Order;
import io.openmarket.order.model.OrderStatus;
import io.openmarket.transaction.model.TransactionErrorType;
import io.openmarket.transaction.model.TransactionStatus;
import io.openmarket.transaction.model.TransactionTaskResult;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Log4j2
public class OrderUpdateLambda {
    private final OrderDao orderDao;
    private final ItemDao itemDao;

    @Inject
    public OrderUpdateLambda(@NonNull final OrderDao orderDao, @NonNull final ItemDao itemDao) {
        this.orderDao = orderDao;
        this.itemDao = itemDao;
    }

    public void updateOrderStatus(@NonNull final TransactionTaskResult transactionTaskResult) {
        log.info("Processing task {}", transactionTaskResult);
        if (!isTransactionsResultValid(transactionTaskResult)) {
            log.error("Invalid transaction task result: {}", transactionTaskResult);
            return;
        }
        OrderStatus orderStatus = OrderStatus.PAYMENT_NOT_RECEIVED;
        if (transactionTaskResult.getError().equals(TransactionErrorType.NONE)) {
            if (transactionTaskResult.getStatus().equals(TransactionStatus.COMPLETED)) {
                orderStatus = OrderStatus.PAYMENT_CONFIRMED;
            } else {
                log.warn("Received a transaction with error None and status {}",
                        transactionTaskResult.getStatus());
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
        if (orderStatus.equals(OrderStatus.PAYMENT_NOT_RECEIVED)) {
            log.info("Payment not received for order {}, items will be rolled back", order.getOrderId());
            rollbackOrderedItems(order);
        }
        order.setStatus(orderStatus);
        orderDao.save(order);
        log.info("Updated order status for orderId {} to {}", order.getOrderId(), orderStatus);
    }

    private void rollbackOrderedItems(@NonNull final Order order) {
        final Map<Integer, Integer> rollbackItemMap = new HashMap<>();
        for (ItemInfo item : order.getItems()) {
            rollbackItemMap.put(item.getItemId(), -1 * item.getQuantity());
        }
        final List<Integer> failedItemIds = itemDao.updateItemStock(rollbackItemMap);
        if (!failedItemIds.isEmpty()) {
            log.error("Failed to rollback some items: {}, updateMap: {}", failedItemIds, rollbackItemMap);
        } else {
            log.info("Successfully rolled back with item map {}", rollbackItemMap);
        }
    }

    private static boolean isTransactionsResultValid(final TransactionTaskResult transactionTaskResult) {
        return !transactionTaskResult.getTransactionId().isEmpty();
    }
}
