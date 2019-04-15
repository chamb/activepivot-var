/*
 * (C) Quartet FS 2007-2014
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import static com.activeviam.var.generator.VaRDataGenerator.CSV_SEPARATOR;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.time.LocalDate;

import com.quartetfs.fwk.IClone;
import com.quartetfs.fwk.format.impl.DateFormatter;
import com.quartetfs.fwk.format.impl.LocalDateParser;

/**
 * <b>TradeDefinition</b>
 *
 * This class is used in order to build a simple trade.
 * @author Quartet Financial Systems
 *
 */
public class Trade implements IClone<Trade>, Serializable{

	/** serialVersionUID */
	private static final long serialVersionUID = -3147979015813031848L;

	/**
	 * The pattern used for parsing/formatting dates
	 */
	public static final String DATE_PATTERN = LocalDateParser.DEFAULT_PATTERN;

	/**
	 * The CSV format to parse/format dates.
	 */
	public static final DateFormatter TRADE_CSV_DATE_FORMAT = new DateFormatter(DATE_PATTERN);

	/** Unique identifier of the trade */
	protected long id;
	protected int book;
	protected String desk;
	protected String trader;
	protected LocalDate date;
	protected String status;
	protected Object dateBucket;
	protected String isSimulated;
	protected int productId;
	protected int productQtyMultiplier;

	/** The counterparty associated with the trade */
	protected String counterparty;

	/** Default constructor */
	public Trade() { }

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder(getClass().getSimpleName());
		sb.append(" (id=").append(getId());
		sb.append(", productId=").append(getProductId());
		sb.append(", status=").append(getStatus());
		sb.append(", book=").append(getBook());
		sb.append(", trader=").append(getTrader());
		sb.append(", isSimulated=").append(getIsSimulated());
		sb.append(", date=").append(getDate());
		sb.append(", counterparty=").append(getCounterparty());
		sb.append(")");
		return sb.toString();
	}

	/**
	 * Compute a CSV representation of this object. For simplier loading of the
	 * CSV files, the fields are aligned with the one in the datastore.
	 *
	 * @return A CSV String representing this object.
	 */
	public String toCsvString() {
		StringWriter sw = new StringWriter();
		PrintWriter writer = new PrintWriter(sw);
		appendCsvRow(writer);
		return sw.toString();
	}
	
	/**
	 * Append a CSV representation of this object into a writer.
	 */
	public void appendCsvRow(PrintWriter sb) {
		sb.print(getId());
		sb.append(CSV_SEPARATOR).print(getProductId());
		sb.append(CSV_SEPARATOR).print(getProductQtyMultiplier());
		sb.append(CSV_SEPARATOR).append(getDesk());
		sb.append(CSV_SEPARATOR).print(getBook());
		sb.append(CSV_SEPARATOR).append(getTrader());
		sb.append(CSV_SEPARATOR).append(getCounterparty());
		sb.append(CSV_SEPARATOR).append(TRADE_CSV_DATE_FORMAT.format(getDate()));
		sb.append(CSV_SEPARATOR).append(getStatus());
		sb.append(CSV_SEPARATOR).append(getIsSimulated());
	}
	
	public static void appendCsvHeader(PrintWriter sb) {
		sb.append("Id");
		sb.append(CSV_SEPARATOR).append("ProductId");
		sb.append(CSV_SEPARATOR).append("ProductQtyMultiplier");
		sb.append(CSV_SEPARATOR).append("Desk");
		sb.append(CSV_SEPARATOR).append("Book");
		sb.append(CSV_SEPARATOR).append("Trader");
		sb.append(CSV_SEPARATOR).append("Counterparty");
		sb.append(CSV_SEPARATOR).append("Date");
		sb.append(CSV_SEPARATOR).append("Status");
		sb.append(CSV_SEPARATOR).append("IsSimulated");
	}


	@Override
	public Trade clone(){
		try{
			Trade clone= (Trade) super.clone();
			return clone;
		}catch (CloneNotSupportedException e){
			return null;
		}
	}

	/**
	 * @return The id
	 */
	public long getId() {
		return this.id;
	}

	/**
	 * Sets the id
	 * @param id The id to set
	 */
	public void setId(long id) {
		this.id = id;
	}

	/**
	 * @return The book
	 */
	public int getBook() {
		return this.book;
	}

	/**
	 * Sets the book
	 * @param book The book to set
	 */
	public void setBook(int book) {
		this.book = book;
	}

	/**
	 * @return The desk
	 */
	public String getDesk() {
		return this.desk;
	}

	/**
	 * Sets the desk
	 * @param desk The desk to set
	 */
	public void setDesk(String desk) {
		this.desk = desk;
	}
	
	/**
	 * @return The trader
	 */
	public String getTrader() {
		return this.trader;
	}

	/**
	 * Sets the trader
	 * @param trader The trader to set
	 */
	public void setTrader(String trader) {
		this.trader = trader;
	}

	/**
	 * @return The counterparty
	 */
	public String getCounterparty() {
		return this.counterparty;
	}

	/**
	 * Sets the counterparty
	 *
	 * @param counterparty The counterParty to set
	 */
	public void setCounterparty(String counterParty) {
		this.counterparty = counterParty;
	}

	/**
	 * @return The date
	 */
	public LocalDate getDate() {
		return this.date;
	}

	/**
	 * Sets the date
	 *
	 * @param date The date to set
	 */
	public void setDate(LocalDate date) {
		this.date = date;
	}

	/**
	 * @return The status
	 */
	public String getStatus() {
		return this.status;
	}

	/**
	 * Sets the status
	 * @param status The status to set
	 */
	public void setStatus(String status) {
		this.status = status;
	}

	/**
	 * @return The dateBucket
	 */
	public Object getDateBucket() {
		return this.dateBucket;
	}

	/**
	 * Sets the dateBucket
	 * @param dateBucket The dateBucket to set
	 */
	public void setDateBucket(Object dateBucket) {
		this.dateBucket = dateBucket;
	}

	/**
	 * @return The isSimulated
	 */
	public String getIsSimulated() {
		return this.isSimulated;
	}

	/**
	 * Sets the isSimulated
	 * @param isSimulated The isSimulated to set
	 */
	public void setIsSimulated(String isSimulated) {
		this.isSimulated = isSimulated;
	}

	/**
	 * @return The productId
	 */
	public int getProductId() {
		return this.productId;
	}

	/**
	 * Sets the productId
	 * @param productId The productId to set
	 */
	public void setProductId(int productId) {
		this.productId = productId;
	}

	/**
	 * @return The productQtyMultiplier
	 */
	public int getProductQtyMultiplier() {
		return this.productQtyMultiplier;
	}

	/**
	 * Sets the productQtyMultiplier
	 * @param productQtyMultiplier The productQtyMultiplier to set
	 */
	public void setProductQtyMultiplier(int productQtyMultiplier) {
		this.productQtyMultiplier = productQtyMultiplier;
	}

	@Override
	public int hashCode() {
		return (int) (id ^ (id >>> 32));
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Trade other = (Trade) obj;
		if (id != other.id) {
			return false;
		}
		return true;
	}

}