# activepivot-var
ActivePivot application with a simple Value at Risk (VaR) model (trades, products, pnl vectors).

By default the data is generated on the fly and loaded directly into ActivePivot. This allows to run large benchmarks without requiring a fast storage to load data files from. The data generation parameters can be modified by editing `src/main/resources/application.properties`.

The project also comes with a data generator that you can run to generate CSV files.
The data generator executable is `com.activeviam.var.generator.VaRDataGenerator` and can be configured with the `src/main/resources/application.properties` file.

The application is packaged with Apache Maven and deployed with Spring Boot. You can launch the application from an IDE such as Eclipse or IntelliJ, by launching the `com.activeviam.var.ActivePivotVarApplication` main class. Or you can build the application with maven, which will generate a Spring Boot "fat jar" that you can execute from the command line: `java -jar activepivot-var-1.0.0-SNAPSHOT.jar`.
