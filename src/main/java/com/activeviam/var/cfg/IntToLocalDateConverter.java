/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.var.cfg;

import com.activeviam.parquet.IParquetParentContainer;
import com.activeviam.var.generator.Trade;
import org.apache.parquet.io.api.PrimitiveConverter;

public class IntToLocalDateConverter extends PrimitiveConverter {

  /**
   * The parent to which the value will be contributed.
   */
  protected final IParquetParentContainer parent;

  public IntToLocalDateConverter(final IParquetParentContainer parent) {
    this.parent = parent;
  }

  @Override
  public void addInt(int value) {
    this.parent.add(Trade.decodeLocalDate(value));
  }

  @Override
  public void addLong(long value) {
    addInt(Math.toIntExact(value));
  }

}
