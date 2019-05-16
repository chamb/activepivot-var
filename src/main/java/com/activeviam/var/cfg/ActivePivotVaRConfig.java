/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.cfg;

import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

import com.activeviam.copper.Registrations;
import com.activeviam.var.cfg.datastore.DatastoreDescriptionConfig;
import com.activeviam.var.cfg.pivot.ActivePivotManagerConfig;
import com.activeviam.var.cfg.security.CorsFilterConfig;
import com.activeviam.var.cfg.security.SecurityConfig;
import com.qfs.server.cfg.IDatastoreConfig;
import com.qfs.server.cfg.i18n.impl.LocalI18nConfig;
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
import org.springframework.core.env.Environment;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

/**
 * Spring configuration of the Nano ActivePivot Application services.<br>
 * Some parameters of the Nano Services can be quickly changed by modifying the nano.properties
 * file.
 *
 * @author ActiveViam
 *
 */
@PropertySource(value = { "classpath:data.properties", "classpath:jwt.properties" })
@Configuration
@Import(value = {
		ActivePivotConfig.class,
		DatastoreConfig.class,
		NoSecurityDatastoreServiceConfig.class,
		DatastoreDescriptionConfig.class,
		FullAccessBranchPermissionsManagerConfig.class,
		DataLoadingConfig.class,
		ActivePivotManagerConfig.class,
		LocalContentServiceConfig.class,
		LocalI18nConfig.class,
		SecurityConfig.class,
		CorsFilterConfig.class,
		ActiveUIResourceServerConfig.class,

		ActivePivotServicesConfig.class,
		ActiveViamRestServicesConfig.class,
		ActiveViamWebSocketServicesConfig.class,
		JwtConfig.class
})
public class ActivePivotVaRConfig {

	private static final Logger LOGGER = Logger.getLogger(ActivePivotVaRConfig.class.getName());

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

	/** Spring environment */
	@Autowired
	protected Environment env;


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

	/**
	 *
	 * Dumps the memory statistics after startup.
	 *
	 * @return void
	 * @throws Exception
	 */
	@Bean
	@DependsOn(value = "startManager")
	public Void dumpStatistics() throws Exception {
		// only dump stats if we want them dumped
		final String dump = env.getProperty("memoryStat.dump");
		if (dump == null || !Boolean.parseBoolean(dump)){
			return null;
		}
		final IActivePivotManager manager = apConfig.activePivotManager();
		MemoryAnalysisService service = new MemoryAnalysisService((IDatastore)manager.getDatastore(), manager, manager.getDatastore().getEpochManager(), Paths.get(System.getProperty("java.io.tmpdir")));
		final Path dumpFolder = service.exportMostRecentVersion("VaR_application");
		LOGGER.info("Memory statistics dumped into "+dumpFolder);
		return null;
	}

}
