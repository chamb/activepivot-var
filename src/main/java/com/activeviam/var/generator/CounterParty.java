/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import static com.activeviam.var.generator.VaRDataGenerator.CSV_SEPARATOR;

import java.io.Serializable;

/**
 * All the information about a counterparty
 *
 * @author Quartet FS
 */
public class CounterParty implements Serializable {

	private static final long serialVersionUID = -3123801814847782041L;

	/** Counterparty name */
	public String counterParty;

	/** Counterparty group */
	public String counterPartyGroup;

	/** Counterparty city */
	public String city;

	/** Counterparty sector */
	public String sector;

	/** Counterparty rating */
	public String rating;

	/**
	 * Constructor
	 *
	 * @param counterParty the counterParty name
	 * @param counterPartyGroup the counterParty group
	 * @param sector the counterParty sector
	 * @param city the counterParty city
	 * @param rating the counterParty rating
	 */
	public CounterParty(String counterParty, String counterPartyGroup, String sector, String city, String rating) {
		this.counterParty = counterParty;
		this.counterPartyGroup = counterPartyGroup;
		this.sector = sector;
		this.city = city;
		this.rating = rating;
	}

	/**
	 * Default constructor
	 */
	public CounterParty() {}

	/**
	 * @return the counterparty name
	 */
	public String getCounterparty() {
		return counterParty;
	}

	/**
	 * @param counterParty the counterParty name
	 */
	public void setCounterParty(String counterParty) {
		this.counterParty = counterParty;
	}

	/**
	 * @return the counterparty group
	 */
	public String getCounterPartyGroup() {
		return counterPartyGroup;
	}

	/**
	 * @param counterPartyGroup the counterparty group
	 */
	public void setCounterPartyGroup(String counterPartyGroup) {
		this.counterPartyGroup = counterPartyGroup;
	}

	/**
	 * @return the counterparty city
	 */
	public String getCity() {
		return city;
	}

	/**
	 * @param city the counterparty city
	 */
	public void setCity(String city) {
		this.city = city;
	}

	/**
	 * @return the counterparty sector
	 */
	public String getSector() {
		return sector;
	}

	/**
	 * @param sector the counterparty sector
	 */
	public void setSector(String sector) {
		this.sector = sector;
	}

	/**
	 * @return the counterparty rating
	 */
	public String getRating() {
		return rating;
	}

	/**
	 * @param rating the counterparty rating
	 */
	public void setRating(String rating) {
		this.rating = rating;
	}

	@Override
	public String toString() {
		return "CounterParty [counterParty="
				+ counterParty
				+ ", counterPartyGroup="
				+ counterPartyGroup
				+ ", country="
				+ city
				+ ", sector="
				+ sector
				+ ", rating="
				+ rating
				+ "]";
	}

	/**
	 * Convert this counterparty to a CSV format
	 *
	 * @return the CSV representation of the counterparty
	 */
	public String toCsvString() {
		StringBuilder sb = new StringBuilder();
		sb.append(getCounterparty());
		sb.append(CSV_SEPARATOR).append(getCounterPartyGroup());
		sb.append(CSV_SEPARATOR).append(getCity());
		sb.append(CSV_SEPARATOR).append(getSector());
		sb.append(CSV_SEPARATOR).append(getRating());
		return sb.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((city == null) ? 0 : city.hashCode());
		result = prime * result + ((counterParty == null) ? 0 : counterParty.hashCode());
		result = prime * result + ((counterPartyGroup == null) ? 0 : counterPartyGroup.hashCode());
		result = prime * result + ((rating == null) ? 0 : rating.hashCode());
		result = prime * result + ((sector == null) ? 0 : sector.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CounterParty other = (CounterParty) obj;
		if (city == null) {
			if (other.city != null)
				return false;
		} else if (!city.equals(other.city))
			return false;
		if (counterParty == null) {
			if (other.counterParty != null)
				return false;
		} else if (!counterParty.equals(other.counterParty))
			return false;
		if (counterPartyGroup == null) {
			if (other.counterPartyGroup != null)
				return false;
		} else if (!counterPartyGroup.equals(other.counterPartyGroup))
			return false;
		if (rating == null) {
			if (other.rating != null)
				return false;
		} else if (!rating.equals(other.rating))
			return false;
		if (sector == null) {
			if (other.sector != null)
				return false;
		} else if (!sector.equals(other.sector))
			return false;
		return true;
	}

}
