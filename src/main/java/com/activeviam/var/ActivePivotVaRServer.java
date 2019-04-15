/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.DispatcherType;

import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.annotations.ClassInheritanceHandler;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.WebAppContext;
import org.springframework.web.WebApplicationInitializer;

import com.activeviam.var.cfg.WebAppInitializer;

/**
 * @author ActiveViam
 *
 */
public class ActivePivotVaRServer {

	/** Root of the web application files, defined relatively to the project root */
	protected static final String WEBAPP = "src/main/webapp";

	/** Jetty server default port (9090) */
	public static final int DEFAULT_PORT = 9090;
	
	/** Create and configure a Jetty Server */
	public static class JettyAnnotationConfiguration extends AnnotationConfiguration {

		@Override
		public void preConfigure(final WebAppContext context) throws Exception {
			final Set<String> set = Collections.singleton(WebAppInitializer.class.getName());
			final Map<String, Set<String>> map = new ClassInheritanceMap();
			map.put(WebApplicationInitializer.class.getName(), set);
			context.setAttribute(CLASS_INHERITANCE_MAP, map);
			_classInheritanceHandler = new ClassInheritanceHandler(map);
		}

	}
	
	public static Server createServer(final int port) {
		final WebAppContext root = new WebAppContext();
		root.setConfigurations(new Configuration[] {new JettyAnnotationConfiguration() });
		root.setContextPath("/");
		root.setParentLoaderPriority(true);
		root.setResourceBase(WEBAPP);

		// Enable GZIP compression
		final GzipHandler gzipHandler = new GzipHandler();
		gzipHandler.setIncludedMimeTypes(
				"text/html",
				"text/xml",
				"text/javascript",
				"text/css",
				"application/x-java-serialized-object",
				"application/json",
				"application/javascript",
				"image/png",
				"image/svg+xml",
				"image/jpeg");
		gzipHandler.setIncludedMethods(HttpMethod.GET.asString(), HttpMethod.POST.asString());
		gzipHandler.setIncludedPaths("/*");
		gzipHandler.setDispatcherTypes(EnumSet.of(DispatcherType.REQUEST));
		root.setGzipHandler(gzipHandler);

		// Create server and configure it
		final Server server = new Server(port);
		server.setHandler(root);

		return server;
	}

	/**
	 * Configure and launch the standalone server.
	 * 
	 * @param args
	 *            only one optional argument is supported: the server port
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {

		int port = DEFAULT_PORT;
		if (args != null && args.length >= 1) {
			port = Integer.parseInt(args[0]);
		}

		final Server server = createServer(port);

		// Launch the server
		server.start();
		server.join();
	}


}
