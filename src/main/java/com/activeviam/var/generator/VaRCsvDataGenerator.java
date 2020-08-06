/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.springframework.boot.SpringBootConfiguration;

/**
 * Generate ActivePivot Sandbox data files, based on the data.properties configuration.
 *
 * @author ActiveViam
 */
@SpringBootConfiguration
public class VaRCsvDataGenerator extends AVaRDataGenerator {

	/**
	 * CSV field separator
	 */
	public static final char CSV_SEPARATOR = '|';

	/**
	 * CSV vector field separator
	 */
	public static final char CSV_VECTOR_SEPARATOR = ';';

	/**
	 * Base directory to output files, working dir by default
	 */
	static final String BASEDIR = ".";


	public static void main(String[] args) {
		runApplication(VaRCsvDataGenerator.class, args);
	}

	private PrintWriter productWriter;
	private PrintWriter riskWriter;
	private PrintWriter tradeWriter;

	@Override
	protected void startProcess() {
		Path productFile = Paths.get(BASEDIR, "data", "products.csv");
		System.out.println("Creating products into " + productFile.toAbsolutePath());
		Path tradeFile = Paths.get(BASEDIR, "data", "trades.csv");
		System.out.println("Creating trades into " + tradeFile.toAbsolutePath());
		Path riskFile = Paths.get(BASEDIR, "data", "risks.csv");
		System.out.println("Creating risks into " + riskFile.toAbsolutePath());

		// Create the data base directory if it does not exist
		Path dataDir = Paths.get(BASEDIR, "data");
		if (!Files.isDirectory(dataDir)) {
			try {
				Files.createDirectory(dataDir);
			} catch (IOException e) {
				if (!Files.isDirectory(dataDir)) {
					throw new RuntimeException("Cannot create directory " + dataDir, e);
				} // else: Ignore, this was created by another process
			}
		}

		try {
			this.productWriter = new PrintWriter(Files.newBufferedWriter(productFile));
			this.riskWriter = new PrintWriter(Files.newBufferedWriter(tradeFile));
			this.tradeWriter = new PrintWriter(Files.newBufferedWriter(riskFile));
		} catch (IOException e) {
			throw new RuntimeException("Cannot create one of the writers", e);
		}
		Product.appendCsvHeader(this.productWriter);
		this.productWriter.println();
		Trade.appendCsvHeader(this.tradeWriter);
		this.tradeWriter.println();
		Risk.appendCsvHeader(this.riskWriter);
		this.riskWriter.println();

	}

	@Override
	protected void addProduct(final Product product) {
		product.appendCsvRow(this.productWriter);
		this.productWriter.println();
	}

	@Override
	protected void addTrade(final Trade trade) {
		trade.appendCsvRow(this.tradeWriter);
		this.tradeWriter.println();
	}

	@Override
	protected void addRisk(final Risk risk) {
		risk.appendCsvRow(this.riskWriter);
		this.riskWriter.println();
	}

	@Override
	protected void completeProcess() {
		this.riskWriter.close();
		this.tradeWriter.close();
		this.productWriter.close();

		System.out.println(this.productCount + " products generated");
		System.out.println(this.tradeCount + " trades generated");
		System.out.println(this.tradeCount + " risk entries generated");

	}

}
