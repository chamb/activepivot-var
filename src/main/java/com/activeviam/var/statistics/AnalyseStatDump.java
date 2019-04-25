/*
 * (C) Quartet FS 2007-2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.statistics;

import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.visitor.impl.AMemoryStatisticWithPredicate;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author ActiveViam
 */
public class AnalyseStatDump {

	public static void main(String[] args){
		if (args.length!=1){
			System.out.println("This program accepts one and only one argument, the path to the statistics dump folder.");
			return;
		}
		File dumpFolder = new File(args[0]);
		try {
			final File[] dumpFiles = dumpFolder.listFiles();
			for (File dumpFile : dumpFiles) {
				if (dumpFile.getName().startsWith("store_")) {
					System.out.println("Reading store file "+dumpFile+" ...");
					final IMemoryStatistic memoryStatistic = MemoryStatisticSerializerUtil.readStatisticFile(dumpFile);
					StoreStatisticRetriever storeRetriever = new StoreStatisticRetriever();
					memoryStatistic.accept(storeRetriever);
					final Map<String, LongAdder> result = storeRetriever.getResult();
					for (String field : result.keySet()) {
						System.out.println("Total off-heap size for field " + field +" of store "+storeRetriever.storeName+": "+
								result.get(field).longValue()
						);
					}
				}else{
					if (dumpFile.getName().startsWith("pivot_")) {
						System.out.println("Reading pivot file "+dumpFile+" ...");
						final IMemoryStatistic memoryStatistic = MemoryStatisticSerializerUtil.readStatisticFile(dumpFile);
						System.out.println("Total off-heap size for pivot: " +
								memoryStatistic.getRetainedOffHeap());
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * A visitor that retrieves the first store statistic it finds.
	 */
	public static class StoreStatisticRetriever extends AMemoryStatisticWithPredicate<Map<String, LongAdder>> {

		Map<String, LongAdder> offHeapByField = new HashMap<>();
		String storeName;

		public StoreStatisticRetriever(){
			super(stat -> stat.getName().equals(MemoryStatisticConstants.STAT_NAME_STORE) || stat.getName().equals(MemoryStatisticConstants.STAT_NAME_CHUNK_OF_CHUNKSET));
		}

		@Override
		public Map<String, LongAdder> getResult(){
			return offHeapByField;
		}

		@Override
		protected boolean match(IMemoryStatistic statistic) {
			if (statistic.getName().equals(MemoryStatisticConstants.STAT_NAME_STORE)){
				storeName  = statistic.getAttributes().get(MemoryStatisticConstants.ATTR_NAME_STORE_NAME).asText();
				System.out.println("Total off-heap size for store " + storeName + ": " +
						statistic.getRetainedOffHeap());
				return true;
			}

			// we are in the case of a chunk statistic
			String field = statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD).asText();
			long offHeapUsage = statistic.getRetainedOffHeap();
			offHeapByField.compute(field, (k, v)-> {
				if (v==null) {
					v = new LongAdder();
				}
				v.add(offHeapUsage);
				return v;
			});

			return false;
		}
	}

}
