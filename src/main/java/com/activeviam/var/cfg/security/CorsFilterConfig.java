/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.cfg.security;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.context.annotation.Configuration;

import com.qfs.security.cfg.impl.ACorsFilterConfig;

/**
 * Spring configuration for CORS filter for NanoPivot.
 * <p>
 * This NanoPivot CORS filter configuration all request from all server.
 * In production, the method {@link #getAllowedOrigins()} should be modified to contain
 * only the authorised urls
 * @author ActiveViam
 *
 */
@Configuration
public class CorsFilterConfig  extends ACorsFilterConfig {

	@Override
	public Collection<String> getAllowedOrigins() {
		return Arrays.asList();
	}

}
