/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import static com.activeviam.var.generator.VaRCsvDataGenerator.CSV_SEPARATOR;

import com.quartetfs.fwk.IClone;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

/**
 * A financial product.
 *
 * @author Quartet FS
 */
public class Product implements IClone<Product>, Serializable {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -3147979015813031848L;

	/**
	 * Product identifier
	 */
	protected int id;
	protected String productType;
	protected String productName;
	protected double productBaseMtm;
	protected String underlierCode;
	protected String underlierCurrency;
	protected String underlierType;
	protected double underlierValue;
	protected double bumpedMtmUp;
	protected double bumpedMtmDown;
	protected double theta;
	protected double rho;

	public Product(int id) {
		this.id = id;
	}

	/**
	 * @return a string with all the field names, formatted like a CSV file header row.
	 */
	public static String csvHeader() {
		StringWriter sw = new StringWriter();
		PrintWriter writer = new PrintWriter(sw);
		appendCsvHeader(writer);
		return sw.toString();
	}

	public static void appendCsvHeader(PrintWriter sb) {
		sb.append("Id");
		sb.append(CSV_SEPARATOR).append("ProductName");
		sb.append(CSV_SEPARATOR).append("ProductType");
		sb.append(CSV_SEPARATOR).append("UnderlierCode");
		sb.append(CSV_SEPARATOR).append("UnderlierCurrency");
		sb.append(CSV_SEPARATOR).append("UnderlierType");
		sb.append(CSV_SEPARATOR).append("UnderlierValue");
		sb.append(CSV_SEPARATOR).append("ProductBaseMtm");
		sb.append(CSV_SEPARATOR).append("BumpedMtmUp");
		sb.append(CSV_SEPARATOR).append("BumpedMtmDown");
		sb.append(CSV_SEPARATOR).append("Theta");
		sb.append(CSV_SEPARATOR).append("Rho");
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(getClass().getSimpleName());
		sb.append(" (");
		sb.append("id=").append(getId());
		sb.append(", productType=").append(getProductType());
		sb.append(", productName=").append(getProductName());
		sb.append(", productBaseMtm=").append(getProductBaseMtm());
		sb.append(", underlierCode=").append(getUnderlierCode());
		sb.append(", underlierValue=").append(getUnderlierValue());
		sb.append(", underlierCurrency=").append(getUnderlierCurrency());
		sb.append(", underlierType=").append(getUnderlierType());
		sb.append(")");
		return sb.toString();
	}

	@Override
	public Product clone() {
		try {
			Product clone = (Product) super.clone();
			return clone;
		} catch (CloneNotSupportedException e) {
			return null;
		}
	}

	public int getId() {
		return id;
	}

	public void setId(int productId) {
		this.id = productId;
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public String getProductType() {
		return productType;
	}

	public void setProductType(String productType) {
		this.productType = productType;
	}

	public double getBumpedMtmDown() {
		return bumpedMtmDown;
	}

	public void setBumpedMtmDown(double bumpedMtmDown) {
		this.bumpedMtmDown = bumpedMtmDown;
	}

	public double getBumpedMtmUp() {
		return bumpedMtmUp;
	}

	public void setBumpedMtmUp(double bumpedMtmUp) {
		this.bumpedMtmUp = bumpedMtmUp;
	}

	public double getProductBaseMtm() {
		return productBaseMtm;
	}

	public void setProductBaseMtm(double productBaseMtm) {
		this.productBaseMtm = productBaseMtm;
	}

	public String getUnderlierCode() {
		return underlierCode;
	}

	public void setUnderlierCode(String underlierCode) {
		this.underlierCode = underlierCode;
	}

	public String getUnderlierCurrency() {
		return underlierCurrency;
	}

	public void setUnderlierCurrency(String underlierCurrency) {
		this.underlierCurrency = underlierCurrency;
	}

	public String getUnderlierType() {
		return underlierType;
	}

	public void setUnderlierType(String underlierType) {
		this.underlierType = underlierType;
	}

	public double getUnderlierValue() {
		return underlierValue;
	}

	public void setUnderlierValue(double underlierValue) {
		this.underlierValue = underlierValue;
	}

	public double getTheta() {
		return this.theta;
	}

	public void setTheta(double theta) {
		this.theta = theta;
	}

	public double getRho() {
		return this.rho;
	}

	public void setRho(double rho) {
		this.rho = rho;
	}

	@Override
	public int hashCode() {
		return id;
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
		Product other = (Product) obj;
		if (id != other.id) {
			return false;
		}
		return true;
	}

	/**
	 * Compute a CSV representation of this object. For simplier loading of the CSV files, the fields
	 * are aligned with the one in the datastore.
	 *
	 * @return A CSV String representing this object.
	 */
	public String toCsvRow() {
		StringWriter sw = new StringWriter();
		PrintWriter writer = new PrintWriter(sw);
		appendCsvRow(writer);
		return sw.toString();
	}

	/**
	 * Append a CSV representation of this object into a string builder.
	 *
	 * @return A CSV String representing this object.
	 */
	public void appendCsvRow(PrintWriter pw) {
		pw.print(getId());
		pw.append(CSV_SEPARATOR).print(getProductName());
		pw.append(CSV_SEPARATOR).print(getProductType());
		pw.append(CSV_SEPARATOR).print(getUnderlierCode());
		pw.append(CSV_SEPARATOR).print(getUnderlierCurrency());
		pw.append(CSV_SEPARATOR).print(getUnderlierType());
		pw.append(CSV_SEPARATOR).print(getUnderlierValue());
		pw.append(CSV_SEPARATOR).print(getProductBaseMtm());
		pw.append(CSV_SEPARATOR).print(getBumpedMtmUp());
		pw.append(CSV_SEPARATOR).print(getBumpedMtmDown());
		pw.append(CSV_SEPARATOR).print(getTheta());
		pw.append(CSV_SEPARATOR).print(getRho());
	}

}
