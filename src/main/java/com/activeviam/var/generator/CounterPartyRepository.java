/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * Repository of counter parties, shared by several services in the Sandbox application.
 *
 * @author Quartet FS
 *
 */
public class CounterPartyRepository {

	/** Logger **/
	protected static final Logger LOGGER = Logger.getLogger(CounterPartyRepository.class.getName());

	/** Cached products */
	public List<CounterParty> counterParties;

	/**
	 * Default constructor.
	 */
	public CounterPartyRepository() {
		this(CounterPartyGenerator.getNumberOfCounterParties());
	}

	/**
	 * Constructor
	 * @param counterPartyNumber the number of counter parties to hold.
	 * (between 1 (inclusive) and {@link CounterPartyGenerator#getNumberOfCounterParties()}
	 * (inclusive))
	 */
	public CounterPartyRepository(int counterPartyNumber) {
		LOGGER.log(Level.INFO, "Generating " + counterPartyNumber + " random counter parties.");
		CounterPartyGenerator counterPartyGenerator = new CounterPartyGenerator();
		synchronized (this) {
			if (this.counterParties == null) {
				List<CounterParty> counterParties = new ArrayList<>(counterPartyNumber);
				for (int p = 0; p < counterPartyNumber; p++) {
					counterParties.add(counterPartyGenerator.generate(p));
				}
				this.counterParties = counterParties;
			}
		}
	}

	public int getCounterPartyCount() {
		return this.counterParties.size();
	}

	public CounterParty getCounterParty(int counterPartyId) {
		return this.counterParties.get(counterPartyId);
	}

}
