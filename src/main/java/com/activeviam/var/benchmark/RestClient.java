package com.activeviam.var.benchmark;

import com.qfs.QfsWebUtils;
import com.qfs.jackson.impl.JacksonSerializer;
import com.qfs.pivot.json.cellset.JsonCellSetData;
import com.qfs.pivot.json.discovery.CatalogDiscovery;
import com.qfs.pivot.json.discovery.CubeDiscovery;
import com.qfs.pivot.json.discovery.DimensionDiscovery;
import com.qfs.pivot.json.discovery.HierarchyDiscovery;
import com.qfs.pivot.json.discovery.JsonDiscovery;
import com.qfs.pivot.json.query.JsonMdxQuery;
import com.qfs.rest.client.impl.ClientPool;
import com.qfs.rest.client.impl.UserAuthenticator;
import com.qfs.rest.services.IRestService;
import com.qfs.rest.services.impl.JsonRestService;
import com.quartetfs.fwk.serialization.SerializerException;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.qfs.pivot.rest.query.IQueriesRestService.MDX_PATH;
import static com.qfs.server.cfg.impl.ActivePivotRestServicesConfig.CUBE_DISCOVERY_SUFFIX;
import static com.qfs.server.cfg.impl.ActivePivotRestServicesConfig.CUBE_QUERIES_SUFFIX;
import static com.qfs.server.cfg.impl.ActivePivotRestServicesConfig.REST_API_URL_PREFIX;

/**
 *
 * Rest client that connects to the ActivePivot REST services and exchange request and responses in
 * JSon.
 *
 * @author ActiveViam
 *
 */
public class RestClient {

	static final Logger LOGGER = Logger.getLogger(RestClient.class.getName());

	static final Level LOG_LEVEL = Level.FINE;

	/** User name */
	static final String USER = "admin";

	/** User password */
	static final String PASSWORD = "admin";

	/** The rest service to run the queries. */
	protected final IRestService restService;

	/** Base url for Spring HTTP Remoting Services */
	protected final String baseUrl;

	/**
	 * HTTP Connection timeout for MDX query
	 */
	public static final int CONNECTION_TIMEOUT = 30*60*1000;

	/**
	 * Full constructor.
	 * @param port server port
	 */
	public RestClient(final int port) {
		baseUrl = "http://localhost:" + port;
		restService = new JsonRestService(baseUrl, new ClientPool(
				1,
				Collections.singleton(new UserAuthenticator(USER, PASSWORD))));
	}

	public JsonDiscovery discover() throws SerializerException, IOException {
		return restService.path(QfsWebUtils.url(baseUrl, REST_API_URL_PREFIX, CUBE_DISCOVERY_SUFFIX))
				.get()
				.as(JsonDiscovery.class);
	}

	public JsonCellSetData executeMDX(String mdx) throws IOException, SerializerException {
		final JsonMdxQuery mdxQuery = new JsonMdxQuery(mdx, Collections.<String, String> emptyMap());
		final String json = JacksonSerializer.serialize(mdxQuery);
		if (LOGGER.isLoggable(LOG_LEVEL)) {
			LOGGER.log(LOG_LEVEL, "Query Json: " + System.lineSeparator() + json);
		}

		return restService.path(QfsWebUtils.url(baseUrl, REST_API_URL_PREFIX, CUBE_QUERIES_SUFFIX, MDX_PATH))
				.post(Entity.entity(json, MediaType.APPLICATION_JSON_TYPE), CONNECTION_TIMEOUT)
				.as(JsonCellSetData.class);
	}


}
