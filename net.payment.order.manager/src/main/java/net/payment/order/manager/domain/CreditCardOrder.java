package net.payment.order.manager.domain;

import java.math.BigDecimal;
import java.util.Date;

public class CreditCardOrder {

    private String orderId;          // Unique identifier for the order
    private String accountId;        // Account ID linked to the credit card
    private Long creditCardNumber;   // Credit card number used for the order
    private BigDecimal orderAmount;  // The total amount of the order
    private String paymentStatus;    // Status of the payment (e.g., "Pending", "Completed", "Failed")
    private Date orderDate;          // Date and time when the order was placed

    // Getters and setters
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public Long getCreditCardNumber() {
        return creditCardNumber;
    }

    public void setCreditCardNumber(Long creditCardNumber) {
        this.creditCardNumber = creditCardNumber;
    }

    public BigDecimal getOrderAmount() {
        return orderAmount;
    }

    public void setOrderAmount(BigDecimal orderAmount) {
        this.orderAmount = orderAmount;
    }

    public String getPaymentStatus() {
        return paymentStatus;
    }

    public void setPaymentStatus(String paymentStatus) {
        this.paymentStatus = paymentStatus;
    }

    public Date getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(Date orderDate) {
        this.orderDate = orderDate;
    }
}