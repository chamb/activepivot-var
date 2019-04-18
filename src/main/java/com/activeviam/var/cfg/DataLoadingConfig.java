/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.cfg;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import com.qfs.platform.IPlatform;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;

import com.activeviam.var.generator.CounterPartyRepository;
import com.activeviam.var.generator.ProductRepository;
import com.activeviam.var.generator.Risk;
import com.activeviam.var.generator.RiskCalculator;
import com.activeviam.var.generator.Trade;
import com.activeviam.var.generator.TradeGenerator;
import com.activeviam.var.generator.VaRDataGenerator;
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

/**
 * Spring configuration for data sources
 * 
 * @author ActiveViam
 *
 */
public class DataLoadingConfig {

    private static final Logger LOGGER = Logger.getLogger(DataLoadingConfig.class.getSimpleName());

    @Autowired
    protected Environment env;

    @Autowired
    protected IDatastore datastore;


    
	/*
	 * **************************** Data loading *********************************
	 */
    @Bean
    @DependsOn(value = "startManager")
    public Void loadData() throws Exception {
    	if("false".equalsIgnoreCase(env.getProperty("csvSource.enabled", "false"))) {
    		generateAndLoadData();
    	} else {
    		loadDataFromCSV();
    	}
    	return null;
    }
    
    
    /**
     * Generate data on the fly and load it.
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
    	for(int p = 0; p < productRepository.getProductCount(); p++) {
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
		final int parallelism = env.getProperty("tradeSource.parallelism", Integer.class, IPlatform.CURRENT_PLATFORM.getProcessorCount()/2);

		ForkJoinPool forkJoinPool = null;

		try {
			forkJoinPool = new ForkJoinPool(parallelism);
			forkJoinPool.submit(() ->
    	IntStream.range(0, tradeCount/batchSize).parallel().forEach(batch -> {
    		
	    	IMessageChunk<Object> tradeChunk = tradeMessage.newChunk();
	    	IMessageChunk<Object> riskChunk = riskMessage.newChunk();
    		
			for(int tradeId = batch*batchSize; tradeId < Math.min(tradeCount, (batch + 1) * batchSize); tradeId++) {

				int productId = (int) (tradeId % productCount);
				int counterPartyId = (int) (tradeId % counterPartyCount);
				Trade trade = tradeGenerator.generate(tradeId,
				productRepository.getProduct(productId),
				counterpartyRepository.getCounterParty(counterPartyId));
				
				Risk risk = riskCalculator.execute(trade, productRepository.getProduct(productId));
			
				tradeChunk.append(trade);
				riskChunk.append(risk);
			}
			
	    	tradeMessage.append(tradeChunk);
	    	riskMessage.append(riskChunk);
	    	
    	})).get();

		} finally {
			if (forkJoinPool != null) {
				forkJoinPool.shutdown(); //always remember to shutdown the pool
			}
		}
		
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
    	tradeConfig.setSeparator(VaRDataGenerator.CSV_SEPARATOR);
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
    	productConfig.setSeparator(VaRDataGenerator.CSV_SEPARATOR);
    	productConfig.setNumberSkippedLines(1);
    	
    	ICSVParserConfiguration riskConfig = source.createParserConfiguration(Arrays.asList(
    			"TradeId",
    			"Delta",
    			"Gamma",
    			"Vega",
    			"Pnl",
    			"PnlVector"));
    	riskConfig.setSeparator(VaRDataGenerator.CSV_SEPARATOR);
    	riskConfig.setNumberSkippedLines(1);
    	
    	FileSystemCSVTopicFactory topicFactory = new FileSystemCSVTopicFactory(false);
    	source.addTopic(topicFactory.createDirectoryTopic(
    			"Trades", ".\\data", "glob:*trades*.csv", tradeConfig));
    	source.addTopic(topicFactory.createDirectoryTopic(
    			"Products", ".\\data", "glob:*products*.csv", productConfig));
    	source.addTopic(topicFactory.createDirectoryTopic(
    			"Risks", ".\\data", "glob:*risks*.csv", riskConfig));

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

	private void printStoreSizes() {

		// Print stop watch profiling
		StopWatch.get().printTimings();
		StopWatch.get().printTimingLegend();

		// print sizes
		SchemaPrinter.printStoresSizes(datastore.getHead().getSchema());
	}

}
