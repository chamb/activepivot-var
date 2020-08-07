/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.activeviam.var.cfg;

import com.qfs.content.cfg.impl.ContentServerRestServicesConfig;
import com.qfs.content.service.IContentService;
import com.qfs.pivot.content.IActivePivotContentService;
import com.qfs.pivot.content.impl.ActivePivotContentServiceBuilder;
import com.qfs.server.cfg.content.IActivePivotContentServiceConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration of the <b>Content Service</b> backed by a local <b>Content Server</b>.
 * <p>
 * This configuration imports {@link ContentServerRestServicesConfig} to expose the content
 * service.
 *
 * @author ActiveViam
 */
@Configuration
public class ContentServiceConfig implements IActivePivotContentServiceConfig {

	/**
	 * @return ActivePivot content service used to store context values, calculated members, and
	 * 		ActiveUI settings and bookmarks.
	 */
	@Bean
	@Override
	public IActivePivotContentService activePivotContentService() {
		return new ActivePivotContentServiceBuilder()
				.withoutPersistence()
				.withoutCache()
				.needInitialization(IContentService.ROLE_ROOT, IContentService.ROLE_ROOT)
				.build();
	}

	@Bean
	@Override
	public IContentService contentService() {
		// Return the real content service used by the activePivotContentService instead of the wrapped one
		return activePivotContentService().getContentService().getUnderlying();
	}

}
