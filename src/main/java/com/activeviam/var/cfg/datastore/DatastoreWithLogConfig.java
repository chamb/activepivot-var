/*
 * (C) Quartet FS 2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.cfg.datastore;

import com.qfs.desc.IDatastoreSchemaDescriptionPostProcessor;
import com.qfs.server.cfg.IActivePivotBranchPermissionsManagerConfig;
import com.qfs.server.cfg.IActivePivotConfig;
import com.qfs.server.cfg.IDatastoreConfig;
import com.qfs.server.cfg.IDatastoreDescriptionConfig;
import com.qfs.store.IDatastore;
import com.qfs.store.build.impl.DatastoreBuilder;
import com.qfs.store.impl.SchemaPrinter;
import com.qfs.store.log.ILogConfiguration;
import com.qfs.store.log.ReplayException;
import com.qfs.store.log.impl.LogConfiguration;
import com.qfs.store.transaction.IDatastoreWithReplay;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotDatastorePostProcessor;
import com.quartetfs.fwk.QuartetRuntimeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple {@link IDatastoreConfig} that creates a {@link IDatastore} with log
 * using an {@link Autowired autowired} {@link IDatastoreDescriptionConfig}.
 *
 * @author ActiveViam
 */
@Configuration
public class DatastoreWithLogConfig implements IDatastoreConfig {

	/** {@link IActivePivotConfig} spring configuration */
	@Autowired
	protected IActivePivotConfig apConfig;

	/** The {@link IDatastoreDescriptionConfig} spring configuration.  */
	@Autowired
	protected IDatastoreDescriptionConfig datastoreDescriptionConfig;

	/** the config of the branch permission manager. */
	@Autowired
	protected IActivePivotBranchPermissionsManagerConfig branchPermissionsManagerConfig;

	/** Spring environment */
	@Autowired
	protected Environment env;

	/** The logger */
	private static final Logger LOGGER = Logger.getLogger(DatastoreWithLogConfig.class.getName());

	@Override
	@Bean
	public IDatastore datastore() {
		final String transactionLog = env.getProperty("transactionLog.mode");
		if ("create".equalsIgnoreCase(transactionLog)){
			return datastoreWithLog();
		}
		if ("replay".equalsIgnoreCase(transactionLog)){
			return datastoreWithLogReplay();
		}
		return simpleDatastore();
	}

	/**
	 * Creates a datastore with no transaction log.
	 * @return
	 */
	protected IDatastore simpleDatastore() {
		return new DatastoreBuilder()
				.setSchemaDescription(this.datastoreDescriptionConfig.schemaDescription())
				.setBranchPermissionsManager(branchPermissionsManagerConfig.branchPermissionsManager())
				.addSchemaDescriptionPostProcessors(getDescriptionPostProcessors(
						this.datastoreDescriptionConfig,
						this.apConfig))
				.setEpochManagementPolicy(this.datastoreDescriptionConfig.epochManagementPolicy())
				.build();
	}

	/**
	 * Creates a datastore that produces a transaction log.
	 * @return
	 */
	protected IDatastore datastoreWithLog() {
		final String transactionLogFolder = env.getProperty("transactionLog.folder");
		final ILogConfiguration logConfiguration = new LogConfiguration(transactionLogFolder);
		final IDatastore datastore = new DatastoreBuilder()
				.setSchemaDescription(this.datastoreDescriptionConfig.schemaDescription())
				.setBranchPermissionsManager(branchPermissionsManagerConfig.branchPermissionsManager())
				.setLogConfiguration(logConfiguration)
				.addSchemaDescriptionPostProcessors(getDescriptionPostProcessors(
						this.datastoreDescriptionConfig,
						this.apConfig))
				.setEpochManagementPolicy(this.datastoreDescriptionConfig.epochManagementPolicy())
				.build();
		LOGGER.log(Level.INFO, "Datastore transaction log will be stored in "+transactionLogFolder);
		return datastore;
	}

	/**
	 * Creates a datastore that replays a transaction log.
	 * @return
	 */
	protected IDatastore datastoreWithLogReplay() {
		final String transactionLogFolder = env.getProperty("transactionLog.folder");
		final ILogConfiguration logConfiguration = new LogConfiguration(transactionLogFolder);
		IDatastoreWithReplay dsWithReplay = new DatastoreBuilder()
				.setSchemaDescription(this.datastoreDescriptionConfig.schemaDescription())
				.setBranchPermissionsManager(branchPermissionsManagerConfig.branchPermissionsManager())
				.addSchemaDescriptionPostProcessors(getDescriptionPostProcessors(
						this.datastoreDescriptionConfig,
						this.apConfig))
				.setEpochManagementPolicy(this.datastoreDescriptionConfig.epochManagementPolicy())
				.setReplayConfiguration(logConfiguration)
				.build();
		try {
			final long start = System.nanoTime();
			final IDatastore datastore = dsWithReplay.replay();
			final long replayTime = (System.nanoTime() - start)/1000000;
			LOGGER.log(Level.INFO, String.format("Datastore replay took %s ms.", replayTime));
			SchemaPrinter.printStoresSizes(datastore.getHead().getSchema());
			return datastore;
		} catch (ReplayException e) {
			IDatastore ds = e.getDatastore();
			throw new QuartetRuntimeException("Replay of datastore failed at epoch "+ds.getMostRecentVersion());
		}
	}

	/**
	 * Returns the {@link IDatastoreSchemaDescriptionPostProcessor post-processors}
	 * to apply to the datastore schema description.
	 *
	 * <p> They are retrieved from the {@link IDatastoreDescriptionConfig}, and the
	 * {@link ActivePivotDatastorePostProcessor} is added at the end of the returned
	 * array.
	 *
	 * @param dsConfig The datastore configuration
	 * @param apConfig The ActivePivot configuration
	 * @return The post-processors to apply to the datastore schema description.
	 */
	protected IDatastoreSchemaDescriptionPostProcessor[] getDescriptionPostProcessors(
			final IDatastoreDescriptionConfig dsConfig,
			final IActivePivotConfig apConfig)
	{
		// Collect the post-processors defined in the configuration file.
		final Collection<? extends IDatastoreSchemaDescriptionPostProcessor> pps = dsConfig.descriptionPostProcessors();
		final int numPps = pps != null ? pps.size() : 0;

		// Create the array that will hold these post-processors
		// and the ActivePivotDatastorePostProcessor,
		// and populate it.
		final IDatastoreSchemaDescriptionPostProcessor[] postProcessors =
				new IDatastoreSchemaDescriptionPostProcessor[numPps + 1];
		int idx = 0;
		if (numPps > 0) {
			for (final IDatastoreSchemaDescriptionPostProcessor pp: pps) {
				postProcessors[idx++] = pp;
			}
		}
		postProcessors[idx++] = ActivePivotDatastorePostProcessor.createFrom(apConfig.activePivotManagerDescription());

		// Return them.
		return postProcessors;
	}
}
