/*
 * (C) ActiveViam 2007-2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * 
 * Generate ActivePivot Sandbox data files, based on the
 * data.properties configuration.
 * 
 * @author ActiveViam
 *
 */
public class VaRDataGenerator {

	/** CSV field separator */
	public static final char CSV_SEPARATOR = '|';
	
	/** CSV vector field separator */
	public static final char CSV_VECTOR_SEPARATOR = ';';
	
	/** Base directory to output files, working dir by default */
	static final String BASEDIR = ".";
	
	public static void main(String[] args) throws IOException {
		
		Properties prop = new Properties();
		prop.load(VaRDataGenerator.class.getClassLoader().getResourceAsStream("data.properties"));
		
		int tradeCount = Integer.parseInt((String) prop.getOrDefault("tradeSource.tradeCount", "1000"));
		int productCount = Integer.parseInt((String) prop.getOrDefault("tradeSource.productCount", "100"));
		int vectorLength = Integer.parseInt((String) prop.getOrDefault("tradeSource.vectorLength", "260"));
		
		ProductRepository products = new ProductRepository(productCount);
		CounterPartyRepository counterparties = new CounterPartyRepository();
		
		Path productFile = Paths.get(BASEDIR, "data", "products.csv");
		Path tradeFile = Paths.get(BASEDIR, "data", "trades.csv");
		Path riskFile = Paths.get(BASEDIR, "data", "risks.csv");
		
		// Create the data base directory if it does not exist
		Path dataDir = Paths.get(BASEDIR, "data");
		if(!Files.isDirectory(dataDir)) {
			Files.createDirectory(dataDir);
		}
		
		// Write the product file
		try(Writer w = Files.newBufferedWriter(productFile)) {
			PrintWriter pw = new PrintWriter(w);
			Product.appendCsvHeader(pw);
			for(int p = 0; p < products.getProductCount(); p++) {
				pw.append('\n');
				products.getProduct(p).appendCsvRow(pw);
			}
			
			System.out.println(productCount + " products written into " + productFile.toRealPath());
		}

		// Generate the trades and the risk entries, write them into a CSV file
		TradeGenerator tradeGenerator = new TradeGenerator();
		RiskCalculator riskCalculator = new RiskCalculator(vectorLength);
		final int counterPartyCount = counterparties.getCounterPartyCount();

		try(
			Writer tradeWriter = Files.newBufferedWriter(tradeFile);
			Writer riskWriter = Files.newBufferedWriter(riskFile);
		) {
			
			PrintWriter tw = new PrintWriter(tradeWriter);
			PrintWriter rw = new PrintWriter(riskWriter);
			Trade.appendCsvHeader(tw);
			Risk.appendCsvHeader(rw);
			tw.append('\n');
			rw.append('\n');
			
			for (int tradeId = 0; tradeId < tradeCount; tradeId++) {
				int productId = (int) (tradeId % productCount);
				int counterPartyId = (int) (tradeId % counterPartyCount);
				Trade trade = tradeGenerator.generate(tradeId,
					products.getProduct(productId),
					counterparties.getCounterParty(counterPartyId));
				trade.appendCsvRow(tw);
				tw.append('\n');
				
				Risk risk = riskCalculator.execute(trade, products.getProduct(productId));
				risk.appendCsvRow(rw);
				rw.append('\n');
			}
			
			System.out.println(tradeCount + " trades written into " + tradeFile.toRealPath());
			System.out.println(tradeCount + " risk entries written into " + riskFile.toRealPath());
		}

	}

}
