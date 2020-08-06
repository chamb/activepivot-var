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
package com.activeviam.var;

import com.activeviam.var.cfg.ActivePivotVaRConfig;
import javax.servlet.MultipartConfigElement;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.DispatcherServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

/**
 * Spring Boot application launcher.
 *
 * @author ActiveViam
 */
@Configuration
@EnableAutoConfiguration(exclude = {org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration.class})
@EnableWebMvc
@Import({ActivePivotVaRConfig.class})
public class ActivePivotVarApplication {

	public static void main(String[] args) {
		SpringApplication.run(ActivePivotVarApplication.class, args);
	}

	/**
	 * Special beans to make AP work in SpringBoot https://github.com/spring-projects/spring-boot/issues/15373
	 */
	@Bean
	public DispatcherServletRegistrationBean dispatcherServletRegistration(
			DispatcherServlet dispatcherServlet,
			ObjectProvider<MultipartConfigElement> multipartConfig) {
		DispatcherServletRegistrationBean registration = new DispatcherServletRegistrationBean(
				dispatcherServlet, "/*");
		registration.setName("springDispatcherServlet");
		registration.setLoadOnStartup(1);
		multipartConfig.ifAvailable(registration::setMultipartConfig);
		return registration;
	}

}
