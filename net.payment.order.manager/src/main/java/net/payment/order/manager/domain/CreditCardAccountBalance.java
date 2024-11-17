package net.payment.order.manager.domain;

import java.math.BigDecimal;

public class CreditCardAccountBalance {

    private String accountId;      // Account ID
    private Long creditCardNumber; // Credit card number
    private BigDecimal balance;    // Current balance
    private BigDecimal creditLimit; // Credit limit

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

