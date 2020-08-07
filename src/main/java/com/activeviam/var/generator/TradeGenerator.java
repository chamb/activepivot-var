/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import com.quartetfs.biz.pivot.cube.hierarchy.IBucketer;
import com.quartetfs.biz.pivot.cube.hierarchy.axis.impl.DefaultTimeBucketer;
import com.quartetfs.fwk.QuartetRuntimeException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * This class generates random trades.
 *
 * @author Quartet FS
 */
public class TradeGenerator {

	//statuses
	private static final String[] statuses = {"SIMULATION", "MATCHED", "DONE"};

	// desks are mapped with underlying types
	private static final Map<String, String> desks;
	private static final int NB_BOOK = 10;
	//misc params
	private static final int PRODUCTQTY_MAX = 10000;
	private static final int DATE_DEPTH_MAX = 10 * 365; //max generated days, up to 10 years
	private static final String[] TRADER_NAMES = {"John", "Will", "Charles", "Henry", "Stan", "Eric",
			"Sam", "Lucy", "Luke"};

	static {
		desks = new HashMap<>();
		desks.put("EquityIndex", "DeskA");
		desks.put("SingleStock", "DeskB");
	}

	private final LocalDate nowAsLocalDate = LocalDate.now();

	/**
	 * Maps of buckets to bucket dates
	 */
	private final NavigableMap<Long, Object> bucketMap;

	public TradeGenerator() {
		// Create the time bucket map
		IBucketer<Long> dateBucketer = new DefaultTimeBucketer();
		this.bucketMap = dateBucketer
				.createBucketMap(TimeUnit.DAYS.toMillis(nowAsLocalDate.toEpochDay()));
	}

	/**
	 * Main function is only used for debugging and testing the random generation.
	 *
	 * @param args arguments
	 */
	public static void main(String[] args) {
		int count = 10; //number of generated objects
		TradeGenerator generator = new TradeGenerator();
		CounterPartyGenerator counterPartyGenerator = new CounterPartyGenerator();

		for (int i = 0; i < count; i++) {
			int counterPartyId = i % CounterPartyGenerator.getNumberOfCounterParties();
			System.out.println(generator
					.generate(i, new Product(i), counterPartyGenerator.generate(counterPartyId)));
		}
	}

	/**
	 * Randomly generate one trade.
	 *
	 * @param tradeId the id of the trade to generate
	 * @param product the product to attach to the trade
	 * @param counterParty the counter party to attach to the trade
	 * @return the generated trade
	 */
	public Trade generate(long tradeId, Product product, CounterParty counterParty) {
		return generate(tradeId, product, counterParty, ThreadLocalRandom.current());
	}

	/**
	 * Randomly generate one trade.
	 *
	 * @param tradeId the id of the trade to generate
	 * @param product the product to attach to the trade
	 * @param counterParty the counter party to attach to the trade
	 * @param random externally provided random
	 * @return the generated trade
	 */
	public Trade generate(long tradeId, Product product, CounterParty counterParty, Random random) {

		Trade trade = new Trade();
		//id
		trade.setId(tradeId);
		//productId
		trade.setProductId(product.getId());
		//quantity
		trade.setProductQtyMultiplier(random.nextInt(PRODUCTQTY_MAX));
		//desk
		trade.setDesk(getDesk(product.getUnderlierType()));
		//bookId
		trade.setBook(random.nextInt(NB_BOOK));
		//trader
		trade.setTrader(TRADER_NAMES[random.nextInt(TRADER_NAMES.length)]);
		//status
		trade.setStatus(statuses[random.nextInt(statuses.length)]);
		//is simulated
		if ("SIMULATION".equals(trade.getStatus())) {
			trade.setIsSimulated("SIMULATION");
		} else {
			trade.setIsSimulated("LIVE");
		}
		//value date
		final LocalDate generatedDate = nowAsLocalDate.plusDays(random.nextInt(DATE_DEPTH_MAX));
		trade.setDate(generatedDate);
		//Counterparty
		trade.setCounterparty(counterParty.getCounterparty());

		// Bucket the date of the trade
		Entry<Long, Object> ceilingEntry = getBucketEntry(trade.getDate());
		if (ceilingEntry == null) {
			throw new QuartetRuntimeException(
					"There is no bucket large enough to hold: " + trade.getDate());
		} else {
			trade.setDateBucket(ceilingEntry.getValue().toString());
		}

		return trade;
	}

	/**
	 * Retrieve the date bucket associated to a date.
	 *
	 * @param date The date from which the bucket must be found.
	 * @return The bucket.
	 */
	public Object getDateBucket(LocalDate date) {
		return getBucketEntry(date).getValue();
	}

	/**
	 * Retrieves the matching bucket entry for a given date.
	 *
	 * @param d The date.
	 * @return The matching bucket.
	 */
	protected Entry<Long, Object> getBucketEntry(LocalDate date) {
		return this.bucketMap.ceilingEntry(TimeUnit.DAYS.toMillis(date.toEpochDay()));
	}

	/**
	 * Retrieves the desk for a given underlier type.
	 *
	 * @param underlierType The underlier type.
	 * @return The associated desk.
	 */
	protected String getDesk(String underlierType) {
		return desks.get(underlierType);
	}

}
