package net.payment.account.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.payment.account.domain.AccountMessage;
import net.payment.account.service.KafkaProducerService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/accounts")
public class AccountController {

    private final KafkaProducerService kafkaProducerService;

    public AccountController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping
    public String sendMessage(@RequestBody AccountMessage accountMessage) throws JsonProcessingException {
        // Generate a unique key for the message or use any custom key
        String key = "account-" + accountMessage.accountId;

        // Send the AccountMessage object to Kafka
        kafkaProducerService.sendMessage(key, accountMessage);

        return "Message sent successfully";
    }

    @GetMapping
    public String getAccounts()
    {
        return "accounts";
    }
}