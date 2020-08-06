/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import java.util.concurrent.ThreadLocalRandom;


/**
 * Generator of {@link CounterParty}.
 *
 * @author Quartet FS
 */
public class CounterPartyGenerator {

	/**
	 * Static names used to generate counterparties
	 */
	protected static final String[] counterpartyNames = {
			"Unilever ord",
			"Asahi Chemical Ind.",
			"Sumitomo Trust and Banking",
			"Mitsubishi Estate",
			"Cosco Co.",
			"Rockwell Automation  Inc.",
			"Formosa Plastics Corporation",
			"Sumitomo Electric",
			"China Life Insurance Company Limited",
			"HSBC Holdings plc",
			"Total System Services",
			"Reed Elsevier PLC",
			"Mitsubishi Electric Co.",
			"Toyota Industries",
			"BHP Billiton Limited",
			"Tyco International (New)",
			"Mitsui Mining and Sm.",
			"Mitsubishi Tokyo Financial Group",
			"Vale R Doce-PNA (Companhia Vale do Rio Doce SA-CVRD) (ADR)",
			"Cathay Pacific Airways Ltd."};
	/**
	 * Static groups names, aligned with counterparyNames, used to generate group counterparties
	 */
	protected static final String[] counterpartyGroups = {
			"Unilever",
			"Asahi",
			"Sumitomo",
			"Mitsubishi",
			"Cosco",
			"Rockwell",
			"Formosa",
			"Sumitomo",
			"China Life Insurance",
			"HSBC",
			"Total",
			"Reed Elsevier",
			"Mitsubishi",
			"Toyota",
			"BHP Billiton",
			"Tyco",
			"Mitsui",
			"Mitsubishi",
			"Vale R Doce",
			"Cathay"
	};
	/**
	 * CounterParty categories
	 */
	protected static final String[] counterpartyCategory = {
			"Consumer Staples",
			"Materials",
			"Financials",
			"Industrials",
			"Information Technology",
			"Consumer Discretionary",
	};
	/**
	 * Possible ratings for a counterParty
	 */
	protected static final String[] ratings = {"AAA", "AA", "A", "BBB", "BB", "B", "CCC", "CC", "C"};
	/**
	 * Locations for a counterParty
	 */
	protected static final String[] locations = {"Paris", "London", "New York", "Tokyo", "Berlin",
			"Johannesburg"};
	/**
	 * The random used to generate counterParties
	 */
	protected static ThreadLocalRandom random;

	/**
	 * Constructor
	 */
	public CounterPartyGenerator() {
		random = ThreadLocalRandom.current();
	}

	public static int getNumberOfCounterParties() {
		return counterpartyNames.length;
	}

	/**
	 * Generate a new counterParty.
	 *
	 * @param counterPartyId id of the counter party (between 0 (inclusive) and {@link
	 *    #getNumberOfCounterParties()} exclusive)
	 * @return a generated {@link CounterParty counter party}
	 */
	public CounterParty generate(int counterPartyId) {
		CounterParty cp = new CounterParty();

		cp.setCounterParty(counterpartyNames[counterPartyId]);
		cp.setCounterPartyGroup(counterpartyGroups[counterPartyId]);
		cp.setCity(locations[random.nextInt(locations.length)]);
		cp.setRating(ratings[random.nextInt(ratings.length)]);
		cp.setSector(counterpartyCategory[random.nextInt(counterpartyCategory.length)]);

		return cp;
	}

}
