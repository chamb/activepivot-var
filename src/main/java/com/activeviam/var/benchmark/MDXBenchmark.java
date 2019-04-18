package com.activeviam.var.benchmark;

import com.activeviam.var.ActivePivotVaRServer;
import com.qfs.pivot.json.cellset.JsonCellSetData;
import com.quartetfs.biz.pivot.context.impl.QueriesTimeLimit;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.IMDXQuery;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.biz.pivot.server.http.impl.HTTPServiceFactory;
import com.quartetfs.biz.pivot.webservices.IQueriesService;
import com.quartetfs.fwk.impl.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 *
 * Simple MDX benchmark using the RestClient.
 *
 * @author ActiveViam
 *
 */
public class MDXBenchmark {

	/** The number of ns in one ms. */
	static final int NANO_IN_MILLI = 1_000_000;

	/** Number of query iterations (per client) */
	protected int iterations;

	/** Number of concurrent clients */
	protected int clientCount;

	/** MDX queries to run, indexed by their name */
	protected Map<String, String> queries;

	/** The names of the queries, in alphabetical order */
	protected String[] queryNames;

	/** Execute queries in the list sequentially or randomly? */
	protected final boolean random;

	public MDXBenchmark(Map<String, String> queries, int iterations, int clientCount) {
		this(queries, iterations, clientCount, true);
	}

	public MDXBenchmark(Map<String, String> queries, int iterations, int clientCount, boolean random) {
		this.iterations = iterations;
		this.clientCount = clientCount;
		this.queries = queries;
		this.random = random;

		this.queryNames = queries.keySet().stream().sorted().collect(Collectors.toList()).toArray(new String[queries.size()]);
	}

	public void run() {
		List<MDXClient> clients = new ArrayList<>(clientCount);
		for(int i = 0; i < clientCount; i++) {
			clients.add(new MDXClient());
		}
		for(int i = 0; i < clientCount; i++) {
			clients.get(i).start();
		}
		for(int i = 0; i < clientCount; i++) {
			try {
				clients.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		double[] avg_query = new double[queries.size()];
		double[] std_query = new double[queries.size()];
		for (int i = 0; i < clientCount; i++) {
			final MDXClient c = clients.get(i);

			for (int j = 0; j < clients.get(i).getQueryCount(); j++) {
				System.out.println(
						String.format(
								"Client " + i + " took an average of %.2f" + "ms with a standard dev of %.2f" + "ms.",
								c.average(j) / NANO_IN_MILLI,
								c.std(j) / NANO_IN_MILLI) + " for query " + queryNames[j]);
				avg_query[j] += c.average(j);
				std_query[j] += c.std(j);
			}

		}
		for (int i = 0; i < queries.size(); i++) {
			System.out.println(
					String.format(
							"Average time for query " + queryNames[i] + " : %.2f" + " ms with a standard dev of %.2f ms",
							avg_query[i] / clients.size() / NANO_IN_MILLI,
							std_query[i] / clients.size() / NANO_IN_MILLI));
		}
	}

	/** Counts the instantiated MDX clients */
	static final AtomicInteger COUNTER = new AtomicInteger(0);

	/**
	 * Single MDX Client, designed to run
	 * in its own thread, over its own
	 * HTTP connection.
	 */
	public class MDXClient extends Thread {

		/** Query error count */
		int errorCount = 0;

		/** Recorded executions */
		List<QueryExecution> executions[];

		/** Random generator */
		final Random rand;

		@SuppressWarnings("javadoc")
		public MDXClient() {
			super("mdx-client-" + COUNTER.getAndIncrement());
			this.rand = new Random();
		}

		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			RestClient client = new RestClient(ActivePivotVaRServer.DEFAULT_PORT);

			this.executions = new ArrayList[queryNames.length];
			for (int i = 0; i < executions.length; i++) {
				this.executions[i] = new ArrayList<>();
			}

			for(int i = 0; i < iterations; i++) {
				try {
					final Pair<String, Integer> nameAndIndex = selectQuery(queryNames, i);
					String mdx = queries.get(nameAndIndex.getKey());
					long before = System.nanoTime();
					final JsonCellSetData jsonCellSetData = client.executeMDX(mdx);
					long elapsed = System.nanoTime() - before;

					int cellSetSize = jsonCellSetData.getCells() == null ? 0 : jsonCellSetData.getCells().size();
					QueryExecution execution = new QueryExecution(getName(), i, nameAndIndex.getKey(), cellSetSize, elapsed);
					executions[nameAndIndex.getValue()].add(execution);
					System.out.println(execution);

				} catch(Exception e) {
					errorCount++;
					e.printStackTrace();
				}
			}
		}

		Pair<String, Integer> selectQuery(final String[] queryNames, final int iteration) {
			int index;
			if(random) {
				index = rand.nextInt(queryNames.length);
			} else {
				index = iteration % queryNames.length;
			}
			return new Pair<>(queryNames[index], index);
		}

		int getQueryCount() {
			return queries.size();
		}

		/** Query execution entry */
		class QueryExecution {

			final String name;

			final int iteration;

			final String queryName;

			final int cellCount;

			final long elapsed;

			public QueryExecution(String clientName, int iteration, String queryName, int cellCount, long elapsed) {
				this.name = clientName;
				this.iteration = iteration;
				this.queryName = queryName;
				this.cellCount = cellCount;
				this.elapsed = elapsed;
			}


			@Override
			public String toString() {
				return name
						+ ", iteration-"
						+ iteration
						+ ", "
						+ queryName
						+ ", result size = "
						+ cellCount
						+ ", elapsed = "
						+ (elapsed / NANO_IN_MILLI)
						+ "ms";
			}
		}

		@SuppressWarnings("javadoc")
		public long totalTime(int queryIndex) {
			long time = 0;
			final int nbrExecs = executions[queryIndex].size();
			for (int i = nbrExecs / 10; i < nbrExecs; ++i) {
				final QueryExecution qe = executions[queryIndex].get(i);
				time += qe.elapsed;
			}
			return time;
		}

		@SuppressWarnings("javadoc")
		public double average(int queryIndex) {
			// Skip the first 10%
			final int nbrExecs = executions[queryIndex].size() - (executions[queryIndex].size() / 10);
			if (nbrExecs != 0) {
				return (totalTime(queryIndex) / nbrExecs);
			} else {
				return 0;
			}
		}

		@SuppressWarnings("javadoc")
		public double std(int queryIndex) {
			final double avg = average(queryIndex);
			final int nbrRmv = executions[queryIndex].size() / 10;
			final int nbrExecs = executions[queryIndex].size() - nbrRmv;

			double total = 0;
			for (int i = nbrRmv; i < executions[queryIndex].size(); ++i) {
				final QueryExecution qe = executions[queryIndex].get(i);
				total += Math.pow(qe.elapsed - avg, 2);
			}
			if (nbrExecs != 0) {
				return Math.sqrt((total / nbrExecs));
			} else {
				return 0;
			}
		}

	}

	/**
	 * Manual launcher.
	 * A collections of queries is populated, and we launch
	 * an instance of the MDX benchmark that will concurrently execute those
	 * queries from several clients in parallel. Queries in the list can
	 * be executed sequentially, or randomly.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		System.out.println(System.getProperty("java.library.path"));

		int iterations = 10;
		int clientCount = 2;

		Map<String, String> queries = new HashMap<>();
		queries.put("Count",
				"SELECT\n" +
						"  NON EMPTY {\n" +
						"    [Measures].[contributors.COUNT]" +
						"  } ON COLUMNS,\n" +
						"  NON EMPTY Hierarchize(\n" +
						"    DrilldownLevel(\n" +
						"      [Products].[Product].[ALL].[AllMember]\n" +
						"    )\n" +
						"  ) ON ROWS\n" +
						"  FROM [ActivePivot VaR]");
		queries.put("VaR 99",
				"SELECT\n" +
						"  NON EMPTY {\n" +
						"    [Measures].[VaR 99]" +
						"  } ON COLUMNS,\n" +
						"  NON EMPTY Hierarchize(\n" +
						"    DrilldownLevel(\n" +
						"      [Products].[Product].[ALL].[AllMember]\n" +
						"    )\n" +
						"  ) ON ROWS\n" +
						"  FROM [ActivePivot VaR]");
		queries.put("VaR 95",
				"SELECT\n" +
						"  NON EMPTY {\n" +
						"    [Measures].[VaR 95]" +
						"  } ON COLUMNS,\n" +
						"  NON EMPTY Hierarchize(\n" +
						"    DrilldownLevel(\n" +
						"      [Products].[Product].[ALL].[AllMember]\n" +
						"    )\n" +
						"  ) ON ROWS\n" +
						"  FROM [ActivePivot VaR]");

		MDXBenchmark benchmark = new MDXBenchmark(queries, iterations, clientCount);

		long before = System.currentTimeMillis();
		benchmark.run();
		long elapsed = System.currentTimeMillis() - before;

		System.out.println(clientCount + " concurrent clients each executed " + iterations + " queries, in " + elapsed/1000 + " seconds.");
	}

}
