/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * <b>RiskCalculator</b>
 *
 * Computes the different risk attributes, based on attributes from the related trade and product.
 *
 * @author ActiveViam
 */
public class RiskCalculator {

	// constants used in calculation formulas
	private static final double BUMP_SIZE_50 = 0.5;
	private static final double SHIFT_OPERAND = 1.1;
	
	/** Default vector length */
	private static final int DEFAULT_VECTOR_LENGTH = 260;

	//factors used for bumpedMtmDown and bumpedMtmUp perturbation
	private static final double[] FACTORS = { .5, .2, .3, .6 };

	/** Size of the pnl vectors */
	protected final int vectorLength;
	
	
	public RiskCalculator() {
		this(DEFAULT_VECTOR_LENGTH);
	}
	
	public RiskCalculator(int vectorLength) {
		this.vectorLength = vectorLength;
	}

	/**
	 * Generate a risk record for the given trade on the given product
	 * @param trade
	 * @param product
	 * @return risk entry
	 */
	public Risk execute(Trade trade, Product product) {

		final Random random = ThreadLocalRandom.current();

		//calculate the rate change based on underlierValue and its shifted value (hard coded SHIFT_OPERAND)
		double underlierValue = product.getUnderlierValue();

		double underlierValueShifted = underlierValue * SHIFT_OPERAND;
		double rateChange = (underlierValueShifted - underlierValue) / underlierValue;

		//get the pvQtyMultiplier used in delta and pnl calculation
		double qtyMultiplier = trade.getProductQtyMultiplier();

		//get bumpedMtmUp and bumpedMtmDown : this refers to the pv after a +25% and -25% bumps
		//that's why BUMP_SIZE_50 is 50% = (+25%) - (-25%)

		double baseBumpedMtmUp = product.getBumpedMtmUp();
		double baseBumpedMtmDown = product.getBumpedMtmDown();

		double bumpedMtmUp = round(baseBumpedMtmUp + baseBumpedMtmUp * FACTORS[random.nextInt(FACTORS.length)], 2);
		double bumpedMtmDown = round(baseBumpedMtmDown + baseBumpedMtmDown * FACTORS[random.nextInt(FACTORS.length)], 2);

		//calculating and adding new measures that depend on external MarketData (rateChange in our case)

		double difference = ((bumpedMtmUp - bumpedMtmDown) / BUMP_SIZE_50);

		double delta = qtyMultiplier * difference;
		double pnlDelta = rateChange * delta;

		/*
		 * theta, gamma, vega, rho, pnlVega and pnl calculation
		 * this formulas do not match the reality the aim here is to have different values for each measure
		 * in a real project these values can be calculated in the calculator or retrieved form an external pricer
		 *
		 * gamma = delta * random[-.1 , +.1]
		 * vega = delta * random[-1 , +1]
		 * theta = pv * 3 / 365
		 * rho = -pv * 1/150
		 */
		double gamma = delta * nextDouble(-0.1, 0.1, random);
		double vega = delta * nextDouble(-1, 1, random);
		double pnlVega = vega * 0.01;
		double pnl = pnlVega + pnlDelta;

		// generate a pnl vector with a gaussian distribution around the pnl
		double[] pnlVector = new double[vectorLength];
		for(int i = 0; i < vectorLength; i++) {
			pnlVector[i] = 0.2 * pnl * random.nextGaussian();
		}
		
		Risk riskEntry = new Risk(trade.getId(), delta, gamma, vega, pnl, pnlVector);
		return riskEntry;
	}
	
	
	
	/**
	 * round a double
	 * @param what the double to round
	 * @param howmuch rounding precision
	 * @return double the rounded double
	 */
	public static double round(double what, int howmuch) {
		return ( (int)(what * Math.pow(10,howmuch) + .5) ) / Math.pow(10,howmuch);
	}

	/**
	 * nextDouble used to generate a random number in a specific interval
	 * @param min lower boundary
	 * @param max upper boundary
	 * @param rand the random to use
	 * @return double
	 */
	public static double nextDouble(double min, double max, Random rand) {
		double deltaRandom = rand.nextDouble() * (max - min);	// should be in [0, 1) * difference = [0, difference)
		return min + deltaRandom;	// should be in [min, min + difference) = [min, max)
	}
	
}
