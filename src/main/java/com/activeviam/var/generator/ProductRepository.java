/*
 * (C) Quartet FS 2007-2013
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * Repository of products, shared by several services in the Sandbox application.
 *
 * @author Quartet FS
 *
 */
public class ProductRepository {

	/** Logger **/
	protected static final Logger LOGGER = Logger.getLogger(ProductRepository.class.getName());

	/** Cached products */
	public List<Product> products;

	/**
	 * Constructor
	 * @param productCount the number of products to hold
	 */
	public ProductRepository(int productCount) {
		this(productCount, ThreadLocalRandom.current());
	}

	/**
	 * Constructor
	 * @param productCount the number of products to hold
	 * @param random the random to use when generating products
	 */
	public ProductRepository(int productCount, Random random) {
		if (productCount <= 0) {
			throw new IllegalArgumentException("Cannot configure the application without products.");
		}

		LOGGER.log(Level.INFO, "Generating " + productCount + " random products.");
		ProductGenerator productGenerator = new ProductGenerator();
		synchronized (this) {
			if(this.products == null) {
				List<Product> products = new ArrayList<>(productCount);
				for(int p = 0; p < productCount; p++) {
					products.add(productGenerator.generate(p, random));
				}
				this.products = products;
			}
		}
	}

	public int getProductCount() { return this.products.size(); }

	public Product getProduct(int productId) { return this.products.get(productId); }

}