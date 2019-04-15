# activepivot-var
ActivePivot application with a simple Value at Risk (VaR) model (trades, products, pnl vectors).

By default the data is generated on the fly and loaded directly into ActivePivot. This allows to run large benchmarks without requiring a fast storage to load data files from. The data generation parameters can be modified by editing `src/main/resources/data.properties`.

The project also comes with a data generator that you can run to generate CSV files.
The data generator executable is `com.activeviam.var.generator.VaRDataGenerator` and can be configured with the `src/main/resources/data.properties` file.
