package net.payment.balance.manager.domain;

import java.math.BigDecimal;

public class CreditCardAccountBalance {

    private String accountId;      // Account ID
    private Long creditCardNumber; // Credit card number
    private BigDecimal balance;    // Current balance
    private BigDecimal creditLimit; // Credit limit
    private String orderId;          // Unique identifier for the order

    private String paymentStatus;    // Status of the payment (e.g., "Pending", "Completed", "Failed")

    // Getters and Setters
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

    public BigDecimal getBalance() {
        return balance;
    }

    public void setBalance(BigDecimal balance) {
        this.balance = balance;
    }

    public BigDecimal getCreditLimit() {
        return creditLimit;
    }

    public void setCreditLimit(BigDecimal creditLimit) {
        this.creditLimit = creditLimit;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPaymentStatus() {
        return paymentStatus;
    }

    public void setPaymentStatus(String paymentStatus) {
        this.paymentStatus = paymentStatus;
    }

    // Simple toString for printing
    @Override
    public String toString() {
        return "CreditCardAccountBalance{" +
                "accountId='" + accountId + '\'' +
                ", creditCardNumber=" + creditCardNumber +
                ", balance=" + balance +
                ", creditLimit=" + creditLimit +
                '}';
    }
}
