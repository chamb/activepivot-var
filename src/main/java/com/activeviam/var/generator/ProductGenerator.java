/*
 * (C) Quartet FS 2007-2010
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This class generates random {@link Product products}.
 *
 * @author Quartet FS
 *
 */
public class ProductGenerator {

	//currencies
	public static final String[] underlierCurrencies = { "EUR", "USD", "GBP", "JPY", "CHF", "ZAR" };

	//underlier descriptions
	protected static final String[] underlierTypes = { "EquityIndex", "SingleStock" };

	//underlier codes for EquityIndex
	protected static final String[] underlierCodesEquityIndex = {
			"AEX",
			"CECEE",
			"ESTX50",
			"FTEPRA",
			"GDAXI",
			"GSUK",
			"IBEX",
			"MIB30",
			"RDXEUR",
			"SD3E",
			"STXX50" };

	//underlier codes for SingleStock
	protected static final String[] underlierCodesSingleStock = {
		".EPEU",   ".GDAXIP",  "1mEUEB.RBS", "AAH.AS",   "AGFP.PA",  "ALBK.I",    "BASF.DE",  "BKIR.I",  "BNPP.PA", "COR.AS",
		"CRDI.MI", "DTEGn.DE", "ELE.MC",     "ENI.MI",   "EONG.DE",  "ENI.MI",    "EONG.DE",  "FTE.PA",  "HICP",
		"INWS.I",  "IPM.I",    "ISPA.AS",    "KESBV.HE", "LOIM.PA",  "LYOE.PA",   "MANG.DE",  "MMTP.PA", "RWEG_p.DE",
		"SOGN.PA", "SRG.MI",   "SZUG.DE",    "TKAG.DE",  "TUIGn.DE", "VOWG_p.DE", "WBSV.VI"};

	//product types
	protected static final String[] productTypes = { "LISTED", "OTC" };

	//product names
	protected static final String[] OTC_ProductNames = { "EQUITY SWAP", "BARRIER OPTION" };
	protected static final String[] LISTED_ProductNames = { "OPTION", "FUTURE" };

	/**
	 * Randomly generate a product with the given product id.
	 *
	 * @param productId the id of the product to generate
	 * @return product the generated product
	 */
	public Product generate(int productId) {
		return generate(productId, ThreadLocalRandom.current());
	}

	/**
	 * Randomly generate a product with the given product id.
	 *
	 * @param productId the id of the product to generate
	 * @param random random generator to use (this allows to provide a
	 * seeded random for repeatable scenarios).
	 * @return product
	 */
	public Product generate(int productId, Random random) {

		// New product
		Product product = new Product(productId);

		//underlierCurrency
		product.setUnderlierCurrency(underlierCurrencies[random.nextInt(underlierCurrencies.length)]);
		//underlierDescription
		product.setUnderlierType(underlierTypes[random.nextInt(underlierTypes.length)]);
		//underlierCode
		if (product.getUnderlierType().equals("EquityIndex")) {
			product.setUnderlierCode(underlierCodesEquityIndex[random.nextInt(underlierCodesEquityIndex.length)]);
		} else {
			product.setUnderlierCode(underlierCodesSingleStock[random.nextInt(underlierCodesSingleStock.length)]);
		}
		//productType
		product.setProductType(productTypes[random.nextInt(productTypes.length)]);
		//productName
		if("LISTED".equals(product.getProductType())) {
			product.setProductName(LISTED_ProductNames[random.nextInt(LISTED_ProductNames.length)]);
		} else {
			product.setProductName(OTC_ProductNames[random.nextInt(OTC_ProductNames.length)]);
		}

		//underlierValue
		product.setUnderlierValue(RiskCalculator.round(random.nextDouble() * 10000,2));
		//bumpedDown
		product.setBumpedMtmDown(RiskCalculator.round(random.nextDouble(),2));
		//bumpedUp
		product.setBumpedMtmUp(RiskCalculator.round(random.nextDouble(),2));
		//baseMTM
		product.setProductBaseMtm(RiskCalculator.round(RiskCalculator.nextDouble(500, 1000, random),2));
		// Theta
		product.setTheta(product.getProductBaseMtm() * 3d / 365d);
		// Rho
		product.setRho( - product.getProductBaseMtm() / 150d);
		return product;
	}

	/**
	 * Main function is only used for debugging and testing the random generation.
	 * @param args arguments
	 */
	public static void main(String[] args){
		int count = 10; //number of generated objects
		ProductGenerator generator = new ProductGenerator();

		for (int i = 0; i < count; i++) {
			System.out.println(generator.generate(i));
		}
	}

}