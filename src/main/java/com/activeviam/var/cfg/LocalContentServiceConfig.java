/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.cfg;

import static com.activeviam.var.cfg.security.SecurityConfig.ROLE_ADMIN;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.qfs.content.service.IContentService;
import com.qfs.content.service.impl.InMemoryContentService;
import com.qfs.pivot.content.IActivePivotContentService;
import com.qfs.pivot.content.impl.ActivePivotContentServiceBuilder;
import com.qfs.server.cfg.content.IActivePivotContentServiceConfig;

/**
 * Spring configuration of the ContentService.
 * 
 * @author ActiveViam
 *
 */
@Configuration
public class LocalContentServiceConfig implements IActivePivotContentServiceConfig {

	@Override
	@Bean
	public IContentService contentService() {
		return new InMemoryContentService();
	}


	@Override
	@Bean
	public IActivePivotContentService activePivotContentService() {
		return new ActivePivotContentServiceBuilder()
					.with(contentService())
					.withoutCache()
					.needInitialization(ROLE_ADMIN, ROLE_ADMIN)
					.build();
	}

}
