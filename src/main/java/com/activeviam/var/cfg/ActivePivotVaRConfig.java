/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.cfg;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.activeviam.copper.Registrations;
import com.activeviam.var.cfg.datastore.DatastoreDescriptionConfig;
import com.activeviam.var.cfg.pivot.ActivePivotManagerConfig;
import com.activeviam.var.cfg.security.CorsFilterConfig;
import com.activeviam.var.cfg.security.SecurityConfig;
import com.qfs.server.cfg.IDatastoreConfig;
import com.qfs.server.cfg.impl.ActivePivotConfig;
import com.qfs.server.cfg.impl.ActivePivotServicesConfig;
import com.qfs.server.cfg.impl.ActiveViamRestServicesConfig;
import com.qfs.server.cfg.impl.ActiveViamWebSocketServicesConfig;
import com.qfs.server.cfg.impl.DatastoreConfig;
import com.qfs.server.cfg.impl.FullAccessBranchPermissionsManagerConfig;
import com.qfs.server.cfg.impl.JwtConfig;
import com.qfs.service.store.impl.NoSecurityDatastoreServiceConfig;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.monitoring.jmx.impl.JMXEnabler;

/**
 * Spring configuration of the Nano ActivePivot Application services.<br>
 * Some parameters of the Nano Services can be quickly changed by modifying the nano.properties
 * file.
 *
 * @author ActiveViam
 *
 */
@PropertySource(value = { "classpath:jwt.properties" })
@EnableWebMvc
@Configuration
@Import(value = {
		ActivePivotConfig.class,
		DatastoreConfig.class,
		NoSecurityDatastoreServiceConfig.class,
		DatastoreDescriptionConfig.class,
		FullAccessBranchPermissionsManagerConfig.class,
		DataLoadingConfig.class,
		ActivePivotManagerConfig.class,
		ContentServiceConfig.class,
		CustomI18nConfig.class,
		SecurityConfig.class,
		CorsFilterConfig.class,
		ActiveUIResourceServerConfig.class,

		ActivePivotServicesConfig.class,
		ActiveViamRestServicesConfig.class,
		ActiveViamWebSocketServicesConfig.class,
		JwtConfig.class
})
public class ActivePivotVaRConfig {

	/** Before anything else we statically initialize the ActiveViam Registry. */
	static {
		Registry.setContributionProvider(new ClasspathContributionProvider());
		Registrations.registerCopperPluginValues();
	}

	/** ActivePivot spring configuration */
	@Autowired
	protected ActivePivotConfig apConfig;

	/** Datastore spring configuration */
	@Autowired
	protected IDatastoreConfig datastoreConfig;

	/** ActivePivot Service Config */
	@Autowired
	protected ActivePivotServicesConfig apServiceConfig;

	/** Enable JMX monitoring */

	/**
	 * Enable JMX Monitoring for the Datastore
	 *
	 * @return the {@link JMXEnabler} attached the Datastore.
	 */
	@Bean
	public JMXEnabler jmxDatastoreEnabler() {
		return new JMXEnabler(datastoreConfig.datastore());
	}

	/**
	 * Enable JMX Monitoring for ActivePivot Components
	 *
	 * @return the {@link JMXEnabler} attached to the activePivotManager
	 */
	@Bean
	@DependsOn(value = "startManager")
	public JMXEnabler JMXActivePivotEnabler() {
		return new JMXEnabler(apConfig.activePivotManager());
	}


	/**
	 *
	 * Initialize and start the ActivePivot Manager
	 *
	 * @return void
	 * @throws Exception
	 */
	@Bean
	public Void startManager() throws Exception {
		apConfig.activePivotManager().init(null);
		apConfig.activePivotManager().start();
		return null;
	}

}
