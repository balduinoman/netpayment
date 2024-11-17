# 🏦 Net Payment  

**Net Payment** é uma aplicação completa para o controle de cartões de crédito, implementada com **Kafka Streams** e **Kafka**. Ela foi projetada para fornecer soluções robustas em sistemas financeiros que exigem processamento de eventos em tempo real e armazenamento eficiente de estados.

## 📂 Módulos  

A aplicação é dividida em quatro módulos principais:  

- **`Account`**: Gerenciamento de contas de cartão de crédito (criação, atualização, consulta).  
- **`Order`**: Processamento de pedidos realizados com cartões (registro e aprovação).  
- **`Fraud`**: Monitoramento e prevenção de fraudes em transações financeiras.  
- **`Balance`**: Controle e visualização do saldo associado a cada conta de cartão.  

## ⚙️ Tecnologias  

- **Kafka Streams**: Usado para processamento de dados em tempo real, garantindo eficiência e escalabilidade.  
- **Kafka**: Atua como banco de dados, utilizando o conceito de **state store changelog** para persistência e consulta de estados diretamente nos tópicos do Kafka.  

## 🖼️ Arquitetura  

![kafka_streams_net-payment-solution](https://github.com/user-attachments/assets/c9b4898c-3c95-4464-8001-7c35244b8f13)


Toda a arquitetura da solução está ilustrada na imagem disponível neste repositório, detalhando:  
- O fluxo de dados entre os módulos.  
- A utilização de tópicos Kafka.  
- A estrutura e os conceitos de state store.  

Essa arquitetura foi projetada para ser modular, escalável e adequada a sistemas de alta demanda no setor financeiro.  
