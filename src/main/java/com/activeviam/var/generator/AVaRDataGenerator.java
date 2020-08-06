/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;

/**
 * Generate ActivePivot Sandbox data files, based on the data.properties configuration.
 *
 * @author ActiveViam
 */
abstract class AVaRDataGenerator implements CommandLineRunner {

    @Value("${tradeSource.tradeCount:1000}")
    protected int tradeCount;
    @Value("${tradeSource.productCount:100}")
    protected int productCount;
    @Value("${tradeSource.vectorLength:260}")
    protected int vectorLength;

    protected static void runApplication(
            final Class<? extends AVaRDataGenerator> klass,
            final String[] args) {
        // Tell spring that this is not a web application
        System.setProperty("spring.main.web-application-type", "NONE");
        SpringApplication.run(klass, args);
    }

    @Override
    public void run(String... args) throws Exception {
        startProcess();

        final ProductRepository products = new ProductRepository(productCount);

        // Write the product file
        for (int p = 0; p < products.getProductCount(); p++) {
            addProduct(products.getProduct(p));
        }

        // Generate the trades and the risk entries, write them into a CSV file
        TradeGenerator tradeGenerator = new TradeGenerator();
        RiskCalculator riskCalculator = new RiskCalculator(vectorLength);
        CounterPartyRepository counterparties = new CounterPartyRepository();
        final int counterPartyCount = counterparties.getCounterPartyCount();

        for (int tradeId = 0; tradeId < tradeCount; tradeId++) {
            int productId = tradeId % productCount;
            int counterPartyId = tradeId % counterPartyCount;
            Trade trade = tradeGenerator.generate(
                    tradeId,
                    products.getProduct(productId),
                    counterparties.getCounterParty(counterPartyId));
            addTrade(trade);

            Risk risk = riskCalculator.execute(trade, products.getProduct(productId));
            addRisk(risk);
        }

        completeProcess();
    }

    protected void startProcess() {}

    protected abstract void addProduct(Product product);

    protected abstract void addTrade(Trade trade);

    protected abstract void addRisk(Risk risk);

    protected void completeProcess() {
    }

}
