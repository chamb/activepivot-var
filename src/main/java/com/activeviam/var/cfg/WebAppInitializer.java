/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.cfg;

import static org.springframework.security.config.BeanIds.SPRING_SECURITY_FILTER_CHAIN;

import java.util.EnumSet;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration.Dynamic;

import org.springframework.web.WebApplicationInitializer;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.filter.DelegatingFilterProxy;
import org.springframework.web.servlet.DispatcherServlet;

import com.activeviam.var.cfg.security.SecurityConfig;

/**
 * Initializer of the Web Application.
 * 
 * @author ActiveViam
 *
 */
public class WebAppInitializer implements WebApplicationInitializer {

	@Override
	public void onStartup(ServletContext servletContext) throws ServletException {
		// Spring Context Bootstrapping
		final AnnotationConfigWebApplicationContext rootAppContext = new AnnotationConfigWebApplicationContext();
		rootAppContext.register(ActivePivotVaRConfig.class);
		servletContext.addListener(new ContextLoaderListener(rootAppContext));
		servletContext.getSessionCookieConfig().setName(SecurityConfig.COOKIE_NAME);

		// The main servlet/the central dispatcher
		final DispatcherServlet servlet = new DispatcherServlet(rootAppContext);
		servlet.setDispatchOptionsRequest(true);
		final Dynamic dispatcher = servletContext.addServlet("springDispatcherServlet", servlet);
		dispatcher.addMapping("/*");
		dispatcher.setLoadOnStartup(1);

		// Spring Security Filter
		final FilterRegistration.Dynamic springSecurity = servletContext.addFilter(SPRING_SECURITY_FILTER_CHAIN, new DelegatingFilterProxy());
		springSecurity.addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/*");
	}

}
