/*
 * (C) ActiveViam 2007-2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import static com.activeviam.var.generator.VaRDataGenerator.*;

import java.io.PrintWriter;

/**
 * 
 * @author ActiveViam
 *
 */
public class Risk {

	protected final long tradeId;
	protected final double delta;
	protected final double gamma;
	protected final double vega;
	protected final double pnl;
	protected final double[] pnlVector;

	
	public Risk(long tradeId, double delta, double gamma, double vega, double pnl, double[] pnlVector) {
		this.tradeId = tradeId;
		this.delta = delta;
		this.gamma = gamma;
		this.vega = vega;
		this.pnl = pnl;
		this.pnlVector = pnlVector;
	}

	public long getTradeId() {
		return tradeId;
	}
	public double getDelta() {
		return delta;
	}
	public double getGamma() {
		return gamma;
	}
	public double getVega() {
		return vega;
	}
	public double getPnl() {
		return pnl;
	}
	public double[] getPnlVector() {
		return pnlVector;
	}
	
	
	public static void appendCsvHeader(PrintWriter w) {
		w.append("TradeId");
		w.append(CSV_SEPARATOR).append("Delta");
		w.append(CSV_SEPARATOR).append("Gamma");
		w.append(CSV_SEPARATOR).append("Vega");
		w.append(CSV_SEPARATOR).append("Pnl");
		w.append(CSV_SEPARATOR).append("PnlVector");
	}
	
	/**
	 * Append a CSV representation of this object into a writer.
	 */
	public void appendCsvRow(PrintWriter pw) {
		pw.print(getTradeId());
		pw.append(CSV_SEPARATOR).print(getDelta());
		pw.append(CSV_SEPARATOR).print(getGamma());
		pw.append(CSV_SEPARATOR).print(getVega());
		pw.append(CSV_SEPARATOR).print(getPnl());
		pw.append(CSV_SEPARATOR);
		printVector(pw, getPnlVector());
	}
	
	private static void printVector(PrintWriter pw, double[] vector) {
		if(vector != null && vector.length != 0) {
			for(int i = 0; i < vector.length; i++) {
				if(i > 0) {
					pw.append(CSV_VECTOR_SEPARATOR);
				}
				pw.print(vector[i]);
			}
		}
	}
	
	
}
