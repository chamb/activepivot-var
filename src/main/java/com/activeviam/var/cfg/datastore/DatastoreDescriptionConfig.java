/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.cfg.datastore;

import static com.qfs.literal.ILiteralType.*;
import static com.qfs.literal.ILiteralType.INT;
import static com.qfs.literal.ILiteralType.LOCAL_DATE;
import static com.qfs.literal.ILiteralType.LONG;
import static com.qfs.literal.ILiteralType.STRING;

import java.util.Collection;
import java.util.LinkedList;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.desc.IReferenceDescription;
import com.qfs.desc.IStoreDescription;
import com.qfs.desc.impl.DatastoreSchemaDescription;
import com.qfs.desc.impl.ReferenceDescription;
import com.qfs.desc.impl.StoreDescriptionBuilder;
import com.qfs.server.cfg.IDatastoreDescriptionConfig;

/**
 * Spring configuration file that exposes the datastore
 * {@link IDatastoreSchemaDescription description}.
 *
 * @author ActiveViam
 *
 */
@Configuration
public class DatastoreDescriptionConfig implements IDatastoreDescriptionConfig {

	/******************** Formatters ***************************/
	public static final String DATE_FORMAT = "localDate[yyyy-MM-dd]";

	
	@Bean
	public IStoreDescription products() {
		
		return new StoreDescriptionBuilder().withStoreName("Products")
				.withField("Id", INT).asKeyField()
				.withField("ProductName", STRING)
				.withField("ProductType", STRING)
				.withField("UnderlierCode", STRING)
				.withField("UnderlierCurrency", STRING)
				.withField("UnderlierType", STRING)
				.withField("UnderlierValue", DOUBLE)
				.withField("ProductBaseMtm", DOUBLE)
				.withField("BumpedMtmUp", DOUBLE)
				.withField("BumpedMtmDown", DOUBLE)
				.withField("Theta", DOUBLE)
				.withField("Rho", DOUBLE)
				.build();
	}
	
	@Bean
	public IStoreDescription trades() {
		
		return new StoreDescriptionBuilder().withStoreName("Trades")
				.withField("Id", LONG).asKeyField()
				.withField("ProductId", INT)
				.withField("ProductQtyMultiplier", DOUBLE)
				.withField("Desk", STRING)
				.withField("Book", INT)
				.withField("Trader", STRING)
				.withField("Counterparty", STRING)
				.withField("Date", LOCAL_DATE)
				.withField("Status", STRING)
				.withField("IsSimulated", STRING)
				.withModuloPartitioning("Id", 8)
				.build();
	}
	
	/** @return the description of the risk store */
	public IStoreDescription risks() {
		return new StoreDescriptionBuilder().withStoreName("Risks")
				.withField("TradeId", LONG).asKeyField()
				.withField("Pnl", DOUBLE)
				.withField("Delta", DOUBLE)
				.withField("Gamma", DOUBLE)
				.withField("Vega", DOUBLE)
				.withNullableField("PnlVector", "double[]")
				.withModuloPartitioning("TradeId", 8)
				.build();
	}
	
	@Bean
	public Collection<IReferenceDescription> references(){
		final Collection<IReferenceDescription> references = new LinkedList<>();
		references.add(ReferenceDescription.builder()
				.fromStore("Trades")
				.toStore("Products")
				.withName("Trade_To_Product")
				.withMapping("ProductId", "Id")
				.build());
		references.add(ReferenceDescription.builder()
				.fromStore("Risks")
				.toStore("Trades")
				.withName("Risk_To_Trade")
				.withMapping("TradeId", "Id")
				.build());
		return references;
	}

	/**
	 *
	 * Provide the schema description of the datastore.
	 * <p>
	 * It is based on the descriptions of the stores in the datastore, the descriptions of the
	 * references between those stores, and the optimizations and constraints set on the schema.
	 *
	 * @return schema description
	 */
	@Override
	@Bean
	public IDatastoreSchemaDescription schemaDescription() {
		final Collection<IStoreDescription> stores = new LinkedList<>();
		stores.add(products());
		stores.add(trades());
		stores.add(risks());
		return new DatastoreSchemaDescription(stores, references());
	}

}
