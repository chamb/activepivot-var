/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.cfg;

import com.activeviam.cloud.azure.storage.entity.impl.AzureCloudDirectory;
import com.activeviam.cloud.azure.storage.fetch.impl.AzureBlobChannel;
import com.activeviam.cloud.azure.storage.impl.BlobUtil;
import com.activeviam.cloud.entity.ICloudDirectory;
import com.activeviam.cloud.entity.ICloudEntityPath;
import com.activeviam.cloud.fetch.impl.AConcurrentlyFetchingChannel;
import com.activeviam.cloud.fetch.impl.CloudFetchingConfig;
import com.activeviam.parquet.IStoreToParquetMapping;
import com.activeviam.parquet.impl.ParquetParser;
import com.activeviam.parquet.impl.StoreToParquetMappingBuilder;
import com.activeviam.parquet.policy.impl.NoRestrictionParquetPolicy;
import com.activeviam.var.cfg.datastore.DatastoreDescriptionConfig;
import com.activeviam.var.generator.CounterPartyRepository;
import com.activeviam.var.generator.ProductRepository;
import com.activeviam.var.generator.Risk;
import com.activeviam.var.generator.RiskCalculator;
import com.activeviam.var.generator.Trade;
import com.activeviam.var.generator.TradeGenerator;
import com.activeviam.var.generator.VaRCsvDataGenerator;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.qfs.msg.IMessage;
import com.qfs.msg.IMessageChunk;
import com.qfs.msg.csv.ICSVParserConfiguration;
import com.qfs.msg.csv.ICSVSourceConfiguration;
import com.qfs.msg.csv.filesystem.impl.FileSystemCSVTopicFactory;
import com.qfs.msg.csv.impl.CSVSource;
import com.qfs.source.IStoreMessageChannel;
import com.qfs.source.impl.CSVMessageChannelFactory;
import com.qfs.source.impl.POJOMessageChannelFactory;
import com.qfs.store.IDatastore;
import com.qfs.store.impl.SchemaPrinter;
import com.qfs.store.transaction.ITransactionManager;
import com.qfs.util.timing.impl.StopWatch;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;

/**
 * Spring configuration for data sources
 *
 * @author ActiveViam
 */
public class DataLoadingConfig {

	private static final Logger LOGGER = Logger.getLogger(DataLoadingConfig.class.getSimpleName());

	@Autowired
	protected Environment env;

	@Autowired
	protected IDatastore datastore;

	@Value("${csvSource.mode:generate}")
	private String mode;

	@Value("${cloud-source.date-folder}")
	private String dateFolder;
	@Value("${cloud-source.connection-string}")
	private String connectionString;
	@Value("${cloud-source.parallel-files}")
	private int parallelFiles;
	@Value("${cloud-source.parallel-parts}")
	private int parallelParts;

	@Value("${csvSource.data-dir}")
	private String dataPath;

	/*
	 * **************************** Data loading *********************************
	 */
	@Bean
	@DependsOn(value = "startManager")
	public Void loadData() throws Exception {
		switch (this.mode) {
			case "generate":
				generateAndLoadData();
				break;
			case "csv":
				loadDataFromCSV();
				break;
			case "parquet-local":
				loadDataFromParquet(false);
				break;
			case "parquet-cloud":
				loadDataFromParquet(true);
				break;
			default:
				throw new IllegalArgumentException(this.mode);
		}
		return null;
	}


	/**
	 * Generate data on the fly and load it.
	 *
	 * @throws Exception
	 */
	public void generateAndLoadData() throws Exception {

		Integer productCount = env.getProperty("tradeSource.productCount", Integer.class, 100);
		Integer tradeCount = env.getProperty("tradeSource.tradeCount", Integer.class, 1000);
		Integer vectorLength = env.getProperty("tradeSource.vectorLength", Integer.class, 260);
		Integer batchSize = env.getProperty("tradeSource.batchSize", Integer.class, 1000);

		POJOMessageChannelFactory channelFactory = new POJOMessageChannelFactory(datastore);
		IStoreMessageChannel<String, Object> productChannel = channelFactory.createChannel("Products");
		IStoreMessageChannel<String, Object> tradeChannel = channelFactory.createChannel("Trades");
		IStoreMessageChannel<String, Object> riskChannel = channelFactory.createChannel("Risks");

		ProductRepository productRepository = new ProductRepository(productCount);
		CounterPartyRepository counterpartyRepository = new CounterPartyRepository();

		final ITransactionManager tm = datastore.getTransactionManager();

		// Open ActivePivot transaction
		final long before = System.nanoTime();
		tm.startTransaction();

		// Generate and load products
		IMessage<String, Object> productMessage = productChannel.newMessage("Products");
		IMessageChunk<Object> productChunk = productMessage.newChunk();
		for (int p = 0; p < productRepository.getProductCount(); p++) {
			productChunk.append(productRepository.getProduct(p));
		}
		productMessage.append(productChunk);
		productChannel.send(productMessage);

		// Generate and load trades and risks
		// Generate the trades and the risk entries, write them into a CSV file
		TradeGenerator tradeGenerator = new TradeGenerator();
		RiskCalculator riskCalculator = new RiskCalculator(vectorLength);
		final int counterPartyCount = counterpartyRepository.getCounterPartyCount();

		IMessage<String, Object> tradeMessage = tradeChannel.newMessage("Trades");
		IMessage<String, Object> riskMessage = riskChannel.newMessage("Risks");

		// Chunk the range of trades to generate into batches
		// and perform the generation of the batches in parallel.
		IntStream.range(0, tradeCount / batchSize).parallel().forEach(batch -> {

			IMessageChunk<Object> tradeChunk = tradeMessage.newChunk();
			IMessageChunk<Object> riskChunk = riskMessage.newChunk();

			for (int tradeId = batch * batchSize; tradeId < Math.min(tradeCount, (batch + 1) * batchSize);
					tradeId++) {

				int productId = (int) (tradeId % productCount);
				int counterPartyId = (int) (tradeId % counterPartyCount);
				Trade trade = tradeGenerator.generate(
						tradeId,
						productRepository.getProduct(productId),
						counterpartyRepository.getCounterParty(counterPartyId));

				Risk risk = riskCalculator.execute(trade, productRepository.getProduct(productId));

				tradeChunk.append(trade);
				riskChunk.append(risk);
			}

			tradeMessage.append(tradeChunk);
			riskMessage.append(riskChunk);

		});

		tradeChannel.send(tradeMessage);
		riskChannel.send(riskMessage);

		// Commit ActivePivot transaction
		tm.commitTransaction();

		final long elapsed = System.nanoTime() - before;
		LOGGER.info("Data load completed in " + elapsed / 1000000L + "ms");

		printStoreSizes();
	}


	/**
	 * Load CSV files.
	 *
	 * @throws Exception
	 */
	public void loadDataFromCSV() throws Exception {

		CSVSource<Path> source = new CSVSource<>();
		final Properties sourceProps = new Properties();
		final String parserThreads = env.getProperty("csvSource.parserThreads", "4");
		sourceProps.setProperty(ICSVSourceConfiguration.PARSER_THREAD_PROPERTY, parserThreads);
		source.configure(sourceProps);

		ICSVParserConfiguration tradeConfig = source.createParserConfiguration(Arrays.asList(
				"Id",
				"ProductId",
				"ProductQtyMultiplier",
				"Desk",
				"Book",
				"Trader",
				"Counterparty",
				"Date",
				"Status",
				"IsSimulated"));
		tradeConfig.setSeparator(VaRCsvDataGenerator.CSV_SEPARATOR);
		tradeConfig.setNumberSkippedLines(1);

		ICSVParserConfiguration productConfig = source.createParserConfiguration(Arrays.asList(
				"Id",
				"ProductName",
				"ProductType",
				"UnderlierCode",
				"UnderlierCurrency",
				"UnderlierType",
				"UnderlierValue",
				"ProductBaseMtm",
				"BumpedMtmUp",
				"BumpedMtmDown",
				"Theta",
				"Rho"));
		productConfig.setSeparator(VaRCsvDataGenerator.CSV_SEPARATOR);
		productConfig.setNumberSkippedLines(1);

		ICSVParserConfiguration riskConfig = source.createParserConfiguration(Arrays.asList(
				"TradeId",
				"Delta",
				"Gamma",
				"Vega",
				"Pnl",
				"PnlVector"));
		riskConfig.setSeparator(VaRCsvDataGenerator.CSV_SEPARATOR);
		riskConfig.setNumberSkippedLines(1);

		FileSystemCSVTopicFactory topicFactory = new FileSystemCSVTopicFactory(false);
		source.addTopic(topicFactory.createDirectoryTopic(
				"Trades", this.dataPath, "glob:*trades*.csv", tradeConfig));
		source.addTopic(topicFactory.createDirectoryTopic(
				"Products", this.dataPath, "glob:*products*.csv", productConfig));
		source.addTopic(topicFactory.createDirectoryTopic(
				"Risks", this.dataPath, "glob:*risks*.csv", riskConfig));

		CSVMessageChannelFactory<Path> factory = new CSVMessageChannelFactory<Path>(source, datastore);

		final ITransactionManager tm = datastore.getTransactionManager();

		// Load data into ActivePivot
		final long before = System.nanoTime();

		// Transaction for TV data
		tm.startTransaction();

		source.fetch(Arrays.asList(
				factory.createChannel("Trades"),
				factory.createChannel("Products"),
				factory.createChannel("Risks")));

		tm.commitTransaction();

		final long elapsed = System.nanoTime() - before;
		LOGGER.info("Data load completed in " + elapsed / 1000000L + "ms");

		printStoreSizes();

		topicFactory.close();
		source.close();
	}

	private void loadDataFromParquet(final boolean cloud) {
		final var tradeMapping = new StoreToParquetMappingBuilder()
				.onStore(DatastoreDescriptionConfig.TRADE_STORE)
				.feedStoreField("Date")
				.withColumnCalculator()
				.mapToObject((schema) -> {
					final int index = IStoreToParquetMapping.getIndexOfFieldNamed("Date", schema);
					return (record, def) -> Trade.decodeLocalDate(record.getInt(index));
				})
				.build();

		final var productMapping = new StoreToParquetMappingBuilder()
				.onStore(DatastoreDescriptionConfig.PRODUCT_STORE)
				.build();

		final var riskMapping = new StoreToParquetMappingBuilder()
				.onStore(DatastoreDescriptionConfig.RISK_STORE)
				.build();

		final var executor = Executors.newFixedThreadPool(this.parallelFiles);
		try {
			if (cloud) {
				parseCloudDirectories(
						executor,
						riskMapping,
						productMapping,
						tradeMapping);
			} else {
				parseLocalDirectories(
						executor,
						riskMapping,
						productMapping,
						tradeMapping);
			}

			printStoreSizes();
		} catch (Exception e) {
			throw new RuntimeException("Cannot load from Parquet", e);
		} finally {
			executor.shutdown();
		}
	}

	private void parseCloudDirectories(
			final ExecutorService executorService,
			final IStoreToParquetMapping riskMapping,
			final IStoreToParquetMapping productMapping,
			final IStoreToParquetMapping tradeMapping) {
		final CloudBlobClient client = getStorageClient();
		final AzureCloudDirectory container = new AzureCloudDirectory(client, "test-var");
		final ICloudDirectory<CloudBlob> dateDir = container.getSubDirectory(this.dateFolder);
		final ICloudDirectory<CloudBlob> riskDir = dateDir.getSubDirectory("risks");
		final ICloudDirectory<CloudBlob> productDir = dateDir.getSubDirectory("products");
		final ICloudDirectory<CloudBlob> tradeDir = dateDir.getSubDirectory("trades");

		final var partLength = 8 * 1024 * 1024; // 8 MB of slices
		final var config = new CloudFetchingConfig(
				this.parallelParts,
				Math.min(this.parallelParts * 3, 64), // At most 500MB of transient parts
				partLength);
		final var downloadExecutor = Executors.newCachedThreadPool();
		final Function<ICloudEntityPath<CloudBlob>, AConcurrentlyFetchingChannel<CloudBlob>>
				factory = (path) -> new AzureBlobChannel(path, config, downloadExecutor);

		// Load data into ActivePivot
		final long before = System.nanoTime();
		datastore.edit(tm -> {
			try (final var parser = new ParquetParser(
					this.datastore,
					executorService,
					new NoRestrictionParquetPolicy())) {
				parser.parse(riskDir, factory, null, riskMapping);
			}
			try (final var parser = new ParquetParser(
					this.datastore,
					executorService,
					new NoRestrictionParquetPolicy())) {
				parser.parse(productDir, factory, null, productMapping);
			}
			try (final var parser = new ParquetParser(
					this.datastore,
					executorService,
					new NoRestrictionParquetPolicy())) {
				parser.parse(tradeDir, factory, null, tradeMapping);
			}
			tm.forceCommit();
		});

		final long elapsed = System.nanoTime() - before;
		LOGGER.info("Data load completed in " + elapsed / 1000000L + "ms");
	}

	private void parseLocalDirectories(
			final ExecutorService executorService,
			final IStoreToParquetMapping riskMapping,
			final IStoreToParquetMapping productMapping,
			final IStoreToParquetMapping tradeMapping) {
		final Path dataDir = Paths.get(this.dataPath);
		final String riskDir = dataDir.resolve("risks").toAbsolutePath().toString();
		final String productDir = dataDir.resolve("products").toAbsolutePath().toString();
		final String tradeDir = dataDir.resolve("trades").toAbsolutePath().toString();

		// Load data into ActivePivot
		final long before = System.nanoTime();
		datastore.edit(tm -> {
			try (final var parser = new ParquetParser(
					this.datastore,
					executorService,
					new NoRestrictionParquetPolicy())) {
				parser.parse(riskDir, riskMapping);
			} catch (IOException e) {
				throw new RuntimeException("Cannot load folder " + riskDir, e);
			}
			try (final var parser = new ParquetParser(
					this.datastore,
					executorService,
					new NoRestrictionParquetPolicy())) {
				parser.parse(productDir, productMapping);
			} catch (IOException e) {
				throw new RuntimeException("Cannot load folder " + productDir, e);
			}
			try (final var parser = new ParquetParser(
					this.datastore,
					executorService,
					new NoRestrictionParquetPolicy())) {
				parser.parse(tradeDir, tradeMapping);
			} catch (IOException e) {
				throw new RuntimeException("Cannot load folder " + tradeDir, e);
			}
			tm.forceCommit();
		});

		final long elapsed = System.nanoTime() - before;
		LOGGER.info("Data load completed in " + elapsed / 1000000L + "ms");
	}

	private void printStoreSizes() {
		// Print stop watch profiling
		StopWatch.get().printTimings();
		StopWatch.get().printTimingLegend();

		// print sizes
		SchemaPrinter.printStoresSizes(datastore.getHead().getSchema());
	}

	private CloudBlobClient getStorageClient() {
		return BlobUtil.getCloudBlobClient(
				Objects.requireNonNull(this.connectionString, "No connection string"));
	}

}
