/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.var.generator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;

/**
 * Generate ActivePivot Sandbox data files, based on the data.properties configuration.
 *
 * @author ActiveViam
 */
@SpringBootConfiguration
public class VaRParquetDataGenerator extends AVaRDataGenerator {

	@Value("${ptb:15360}")
	protected int tradeBuffer;
	@Value("${ppb:1024}")
	protected int productBuffer;
	@Value("${prb:15360}")
	protected int riskBuffer;

	/**
	 * Base directory to output files, working dir by default.
	 */
	static final String BASEDIR = ".";


	public static void main(String[] args) {
		runApplication(VaRParquetDataGenerator.class, args);
	}

	private ExecutorService executor;
	private LimitedFileWriter productWriter;
	private LimitedFileWriter riskWriter;
	private LimitedFileWriter tradeWriter;

	@Override
	protected void startProcess() {
		var productFolder = Paths.get(BASEDIR, "data", "parquet", "products");
		createFolder(productFolder);
		System.out.println("Creating products into " + productFolder.toAbsolutePath());
		var tradeFolder = Paths.get(BASEDIR, "data", "parquet", "trades");
		createFolder(tradeFolder);
		System.out.println("Creating trades into " + tradeFolder.toAbsolutePath());
		var riskFolder = Paths.get(BASEDIR, "data", "parquet", "risks");
		createFolder(riskFolder);
		System.out.println("Creating risks into " + riskFolder.toAbsolutePath());

		this.executor = Executors.newFixedThreadPool(8);
		this.productWriter = new LimitedFileWriter(
				this.executor,
				productFolder,
				createProductSchema(),
				CompressionCodecName.UNCOMPRESSED,
				this.productBuffer);
		this.riskWriter = new LimitedFileWriter(
				this.executor,
				riskFolder,
				createRiskSchema(),
				CompressionCodecName.SNAPPY,
				this.riskBuffer);
		this.tradeWriter = new LimitedFileWriter(
				this.executor,
				tradeFolder,
				createTradeSchema(),
				CompressionCodecName.SNAPPY,
				this.tradeBuffer);
	}

	private static void createFolder(final Path path) {
		if (!Files.isDirectory(path)) {
			try {
				Files.createDirectories(path);
			} catch (IOException e) {
				if (!Files.isDirectory(path)) {
					throw new RuntimeException("Cannot create directory " + path, e);
				} // else: Ignore, this was created by another process
			}
		}
	}

	@Override
	protected void addProduct(final Product product) {
		this.productWriter.writeRecord(record -> {
			record.put("Id", product.getId());
			record.put("ProductName", product.getProductName());
			record.put("ProductType", product.getProductType());
			record.put("UnderlierCode", product.getUnderlierCode());
			record.put("UnderlierCurrency", product.getUnderlierCurrency());
			record.put("UnderlierType", product.getUnderlierType());
			record.put("UnderlierValue", product.getUnderlierValue());
			record.put("ProductBaseMtm", product.getProductBaseMtm());
			record.put("BumpedMtmUp", product.getBumpedMtmUp());
			record.put("BumpedMtmDown", product.getBumpedMtmDown());
			record.put("Theta", product.getTheta());
			record.put("Rho", product.getRho());
		});
	}

	@Override
	protected void addTrade(final Trade trade) {
		this.tradeWriter.writeRecord(record -> {
			record.put("Id", trade.getId());
			record.put("ProductId", trade.getProductId());
			record.put("ProductQtyMultiplier", trade.getProductQtyMultiplier());
			record.put("Desk", trade.getDesk());
			record.put("Book", trade.getBook());
			record.put("Trader", trade.getTrader());
			record.put("Counterparty", trade.getCounterparty());
			record.put("Date", Trade.encodeLocalDate(trade.getDate())); // Change this to something else
			record.put("Status", trade.getStatus());
			record.put("IsSimulated", trade.getIsSimulated());
		});
	}

	@Override
	protected void addRisk(final Risk risk) {
		this.riskWriter.writeRecord(record -> {
			record.put("TradeId", risk.getTradeId());
			record.put("Pnl", risk.getPnl());
			record.put("Delta", risk.getDelta());
			record.put("Gamma", risk.getGamma());
			record.put("Vega", risk.getVega());
			record.put("PnlVector", risk.getPnlVector());
		});
	}

	@Override
	protected void completeProcess() {
		this.riskWriter.flush();
		this.productWriter.flush();
		this.tradeWriter.flush();

		this.executor.shutdown();

		try {
			this.executor.awaitTermination(1, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted while waiting for write tasks to end", e);
		}

		this.riskWriter.sprinkleSuccessFile();
		this.tradeWriter.sprinkleSuccessFile();
		this.productWriter.sprinkleSuccessFile();

		System.out.println(this.productCount + " products generated");
		System.out.println(this.tradeCount + " trades generated");
		System.out.println(this.tradeCount + " risk entries generated");

	}

	private Schema createProductSchema() {
		final var intS = Schema.create(Type.INT);
		final var doubleS = Schema.create(Type.DOUBLE);
		final var strS = Schema.create(Type.STRING);
		final var list = List.of(
				field("Id", intS),
				field("ProductName", strS),
				field("ProductType", strS),
				field("UnderlierCode", strS),
				field("UnderlierCurrency", strS),
				field("UnderlierType", strS),
				field("UnderlierValue", doubleS),
				field("ProductBaseMtm", doubleS),
				field("BumpedMtmUp", doubleS),
				field("BumpedMtmDown", doubleS),
				field("Theta", doubleS),
				field("Rho", doubleS));
		return Schema.createRecord("products", "", "", false, list);
	}

	private Schema createTradeSchema() {
		final var intS = Schema.create(Type.INT);
		final var longS = Schema.create(Type.LONG);
		final var doubleS = Schema.create(Type.DOUBLE);
		final var strS = Schema.create(Type.STRING);
		final var list = List.of(
				field("Id", longS),
				field("ProductId", intS),
				field("ProductQtyMultiplier", doubleS),
				field("Desk", strS),
				field("Book", intS),
				field("Trader", strS),
				field("Counterparty", strS),
				field("Date", intS),
				field("Status", strS),
				field("IsSimulated", strS));
		return Schema.createRecord("trades", "", "", false, list);
	}


	private Schema createRiskSchema() {
		final var longS = Schema.create(Type.LONG);
		final var doubleS = Schema.create(Type.DOUBLE);
		final var doubleArrayS = Schema.createArray(doubleS);
		final var list = List.of(
				field("TradeId", longS),
				field("Pnl", doubleS),
				field("Delta", doubleS),
				field("Gamma", doubleS),
				field("Vega", doubleS),
				field("PnlVector", doubleArrayS));
		return Schema.createRecord("risks", "", "", false, list);
	}

	private static Field field(final String name, final Schema schema) {
		return new Schema.Field(name, schema, "Field " + name, (Object) null);
	}

	static class LimitedFileWriter {

		private final ExecutorService executor;
		private final Path baseDir;
		private final Schema schema;
		private final  CompressionCodecName codec;

		private long written;
		private int pos;
		private Record[] buffer;

		LimitedFileWriter(
				ExecutorService executor,
				Path baseDir,
				Schema schema,
				final CompressionCodecName codec,
				final int bufferSize) {
			this.executor = executor;
			this.baseDir = baseDir;
			this.schema = schema;
			this.codec = codec;

			this.written = 0;
			this.pos = 0;
			this.buffer = createRecordBuffer(bufferSize);
		}

		private Record[] createRecordBuffer(final int size) {
			return IntStream.range(0, size)
					.mapToObj(i -> new GenericData.Record(this.schema))
					.toArray(Record[]::new);
		}

		void writeRecord(final Consumer<Record> writer) {
		    writer.accept(this.buffer[this.pos]);
		    this.pos += 1;
		    this.written += 1;

		    if (this.pos == this.buffer.length) {
		    	this.pos = 0;
		    	final var buffer = this.buffer;
		    	final var idx = this.written;
		    	this.buffer = createRecordBuffer(1024 * 15);
		    	this.executor.submit(() -> {
					try {
						try (final var fileWriter = createWriter(idx)) {
						    for (final Record record : buffer) {
						    	fileWriter.write(record);
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				});
			}
		}

		void flush() {
			final var buffer = this.buffer;
			this.buffer = null;
			if (this.pos > 0) {
			    final var lastIdx = this.written;
			   final var end = this.pos;
				this.executor.submit(() -> {
					try {
						try (final var fileWriter = createWriter(lastIdx)) {
							for (int i = 0; i < end; i += 1)
								fileWriter.write(buffer[i]);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				});
			}
		}

		void sprinkleSuccessFile() {
			try {
				Files.createFile(this.baseDir.resolve("_SUCCESS"));
			} catch (IOException e) {
				throw new RuntimeException("Cannot create success file");
			}
		}

		private ParquetWriter<Record> createWriter(long idx) {
			final var path = new org.apache.hadoop.fs.Path(
				this.baseDir.resolve(idx + ".parquet").toAbsolutePath().toString());
			try {
				return AvroParquetWriter.<Record>builder(path)
						.withSchema(this.schema)
						.withConf(new Configuration())
						.withCompressionCodec(this.codec)
						.build();
			} catch (IOException e) {
				throw new RuntimeException("Cannot create write to ", e);
			}
		}
	}



}
