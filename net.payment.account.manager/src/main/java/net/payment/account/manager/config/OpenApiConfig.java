package net.payment.account.manager.config;


import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Net Payment Account Manager API")
                        .version("1.0")
                        .description("API documentation for account manager")
                        .contact(new Contact()
                                .email("balduinoman@gmail.com")
                                .url("https://github.com/balduinoman")
                        )
                );
    }
}
