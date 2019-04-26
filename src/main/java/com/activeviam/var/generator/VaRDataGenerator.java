/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import com.qfs.platform.IPlatform;
import com.quartetfs.fwk.QuartetRuntimeException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

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

		final int parallelism = Integer.parseInt((String)prop.getOrDefault("tradeSource.parallelism", Integer.valueOf(IPlatform.CURRENT_PLATFORM.getProcessorCount()).toString()));

		ForkJoinPool forkJoinPool = null;

		try {
			forkJoinPool = new ForkJoinPool(parallelism);
			forkJoinPool.submit(() ->
					IntStream.range(0, parallelism).parallel().forEach(batch -> {
						generateTradeAndRiskFiles(tradeCount, productCount, products, counterparties, tradeGenerator, riskCalculator, batch, tradeCount/parallelism);
					})).get();
		}catch (Exception e){
			e.printStackTrace();
		} finally{
			if (forkJoinPool!=null) {
				forkJoinPool.shutdown();
			}
			forkJoinPool = null;
		}

	}

	private static void generateTradeAndRiskFiles(int totalTradeCount,
												  int totalProductCount,
												  ProductRepository products,
												  CounterPartyRepository counterparties,
												  TradeGenerator tradeGenerator,
												  RiskCalculator riskCalculator,
												  int fileNumber,
												  int batchSize) {
		Path tradeFile = Paths.get(BASEDIR, "data", "trades-" + fileNumber + ".csv");
		Path riskFile = Paths.get(BASEDIR, "data", "risks-"+fileNumber+".csv");
		try (
				Writer tradeWriter = Files.newBufferedWriter(tradeFile);
				Writer riskWriter = Files.newBufferedWriter(riskFile);
		) {

			PrintWriter tw = new PrintWriter(tradeWriter);
			PrintWriter rw = new PrintWriter(riskWriter);
			Trade.appendCsvHeader(tw);
			Risk.appendCsvHeader(rw);
			tw.append('\n');
			rw.append('\n');

			int counterPartyCount = counterparties.getCounterPartyCount();
			final int minTradeId = fileNumber * batchSize;
			final int maxTradeId = Math.min(totalTradeCount, (fileNumber + 1) * batchSize);
			for (int tradeId = minTradeId; tradeId < maxTradeId; tradeId++) {
				int productId = (int) (tradeId % totalProductCount);
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

			System.out.println(maxTradeId-minTradeId + " trades written into " + tradeFile.toRealPath());
			System.out.println(maxTradeId-minTradeId + " risk entries written into " + riskFile.toRealPath());
		}catch (Exception e){
			throw new QuartetRuntimeException("Unable to generate risk and trade files n° "+fileNumber, e);
		}
	}

}
