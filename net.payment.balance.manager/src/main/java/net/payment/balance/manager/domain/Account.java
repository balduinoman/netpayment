package net.payment.balance.manager.domain;

import java.time.LocalDateTime;
import java.util.Date;

public class Account{

    private String accountId;

    private String accountName;

    private LocalDateTime accountLastUpdate;

    private String userDocumentId;

    private Long creditCardNumber;

    private Number securityCode;

    private Date expirationDate;

    private String brandName;

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public LocalDateTime getAccountLastUpdate() {
        return accountLastUpdate;
    }

    public void setAccountLastUpdate(LocalDateTime accountLastUpdate) {
        this.accountLastUpdate = accountLastUpdate;
    }

    public String getAccountName() {
        return accountName;
    }

    public void setAccountName(String userName) {
        this.accountName = userName;
    }

    public String getUserDocumentId() {
        return userDocumentId;
    }

    public void setUserDocumentId(String userDocumentId) {
        this.userDocumentId = userDocumentId;
    }

    public Long getCreditCardNumber() {
        return creditCardNumber;
    }

    public void setCreditCardNumber(Long creditCardNumber) {
        this.creditCardNumber = creditCardNumber;
    }

    public Number getSecurityCode() {
        return securityCode;
    }

    public void setSecurityCode(Number securityCode) {
        this.securityCode = securityCode;
    }

    public Date getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate(Date expirationDate) {
        this.expirationDate = expirationDate;
    }

    public String getBrandName() {
        return brandName;
    }

    public void setBrandName(String brandName) {
        this.brandName = brandName;
    }
}
