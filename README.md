# sandbox-light
ActivePivot application with a simple portfolio management data model (trades, products, risks and aggregations).

The project comes with a data generator that you must run at least once before starting the application.
The data generator executable is `com.activeviam.sandbox.generator.DataGenerator` and can be configured in the `src/main/resources/sandbox.properties` file

Once the three data csv files are generated in the `data` directory, the application can be started. ActivePivot will parse the CSV files and load them into an in-memory cube, ready for analysis.
