/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.var.generator;

import java.util.Arrays;
import java.util.Objects;

public class VaRDataGenerator {

  public static void main(final String[] args) {
  	if (args.length != 1) {
      throw new IllegalArgumentException(
          "Invalid args. Expecting: <mode=csv|parquet>\nGot: " + Arrays.toString(args));
    }

  	switch (Objects.requireNonNull(args[0], "Null mode")) {
      case "csv":
        VaRCsvDataGenerator.main(new String[0]);
        break;
      case "parquet":
        VaRParquetDataGenerator.main(new String[0]);
        break;
      default:
        throw new IllegalArgumentException("Unknown mode: " + args[0]);
    }
  }

}
