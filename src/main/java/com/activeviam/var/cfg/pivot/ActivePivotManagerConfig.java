/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.cfg.pivot;

import com.activeviam.builders.StartBuilding;
import com.activeviam.copper.ICopperContext;
import com.activeviam.copper.api.Copper;
import com.activeviam.desc.build.ICanBuildCubeDescription;
import com.activeviam.desc.build.ICubeDescriptionBuilder.INamedCubeDescriptionBuilder;
import com.activeviam.desc.build.dimensions.ICanStartBuildingDimensions;
import com.activeviam.var.cfg.datastore.DatastoreDescriptionConfig;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.server.cfg.IActivePivotManagerDescriptionConfig;
import com.qfs.vector.IVector;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo.LevelType;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.ISelectionDescription;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * Configuration of the cube, hierarchies and aggregations.
 *
 * @author ActiveViam
 */
@Configuration
public class ActivePivotManagerConfig implements IActivePivotManagerDescriptionConfig {

	/* ********** */
	/* Formatters */
	/* ********** */
	public static final String DOUBLE_FORMAT = "DOUBLE[##.00]";

	@Autowired
	private Environment env;

	/**
	 * Creates the {@link ISelectionDescription} for NanoPivot Schema.
	 *
	 * @param datastoreDescription : The datastore description
	 * @return The created selection description
	 */
	public static ISelectionDescription createNanoPivotSchemaSelectionDescription(
			final IDatastoreSchemaDescription datastoreDescription) {
		return StartBuilding.selection(datastoreDescription)
				.fromBaseStore("Risks")
				.withAllReachableFields()
				.build();
	}

	/**
	 * Creates the cube description.
	 *
	 * @return The created cube description
	 */
	public static IActivePivotInstanceDescription createCubeDescription() {
		return configureCubeBuilder(StartBuilding.cube("ActivePivot VaR")).build();
	}

	/**
	 * Configures the given builder in order to created the cube description.
	 *
	 * @param builder The builder to configure
	 * @return The configured builder
	 */
	public static ICanBuildCubeDescription<IActivePivotInstanceDescription> configureCubeBuilder(
			final INamedCubeDescriptionBuilder builder) {

		return builder
				.withContributorsCount().withAlias("Trade Count")
				.withCalculations(ActivePivotManagerConfig::coPPerCalculations)

				// Define hierarchies of the cube
				.withDimensions(ActivePivotManagerConfig::dimensions);
	}

	/**
	 * Adds the dimensions descriptions to the input builder.
	 *
	 * @param builder The cube builder
	 * @return The builder for chained calls
	 */
	public static ICanBuildCubeDescription<IActivePivotInstanceDescription> dimensions(
			ICanStartBuildingDimensions builder) {

		return builder
				.withDimension("Products")
				.withHierarchy("Underlier")
				.withLevels("UnderlierType", "UnderlierCode")
				.withHierarchy("Product").asDefaultHierarchy()
				.withLevel("ProductType")
				.withLevel("ProductName")
				.withHierarchy("Currency")
				.withLevel("UnderlierCurrency")
				.withFirstObjects("EUR", "GBP", "USD", "JPY")

				.withDimension("Booking")
				.withHierarchyOfSameName().asDefaultHierarchy()
				.withLevels("Desk", "Book")
				.withSingleLevelHierarchy("Traders").withPropertyName("Trader")
				.withSingleLevelHierarchy("Counterparties").withPropertyName("Counterparty")

				.withDimension("Date")
				.withHierarchyOfSameName()
				.withLevel("Date")
				.withType(LevelType.TIME)
				.withFormatter("DATE[yyyy-MM-dd]");
	}

	/**
	 * The CoPPer calculations to add to the cube
	 *
	 * @param context The context with which to build the calculations.
	 */
	public static void coPPerCalculations(final ICopperContext context) {
		ActivePivotManagerConfig.someAggregatedMeasures(context);
	}

	/* ******************* */
	/* Measures definition */
	/* ******************* */

	/**
	 * Define some calculations using the COPPER API.
	 *
	 * @param context The CoPPer build context.
	 * @return The Dataset of the aggregated measures.
	 */
	protected static void someAggregatedMeasures(final ICopperContext context) {
		Stream.of("Pnl", "Delta", "Vega").forEach(field -> {
			Copper.sum(field)
					.withFormatter(DOUBLE_FORMAT)
					.withName(field)
					.publish(context);
		});
		final var vector = Copper.sum("PnlVector").as("PnlVector").publish(context);

		// Value at Risk calculation
		Stream.of(95, 99).forEach(quantile -> {
			vector.mapToDouble((IVector v) -> v.quantileDouble(quantile / 100d))
					.withFormatter(DOUBLE_FORMAT)
					.withName("VaR " + quantile)
					.publish(context);
		});
	}

	@Override
	public IDatastoreSchemaDescription userSchemaDescription() {
		return new DatastoreDescriptionConfig(this.env).schemaDescription();
	}

	@Override
	public IActivePivotManagerDescription userManagerDescription() {
		return StartBuilding.managerDescription("VarManager")
				.withCatalog("ActivePivot Catalog")
				.containingAllCubes()
				.withSchema("ActivePivot Schema")
				.withSelection(createNanoPivotSchemaSelectionDescription(userSchemaDescription()))
				.withCube(createCubeDescription())
				.build();
	}


}
