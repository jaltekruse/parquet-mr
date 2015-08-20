/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.deltastrings;

import java.io.IOException;

import org.junit.Test;
import org.junit.Assert;

<<<<<<< HEAD:parquet-column/src/test/java/parquet/column/values/deltastrings/TestDeltaByteArray.java
import parquet.bytes.DirectByteBufferAllocator;
import parquet.column.values.Utils;
import parquet.column.values.ValuesReader;
import parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import parquet.io.api.Binary;
=======
import org.apache.parquet.column.values.Utils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.io.api.Binary;
>>>>>>> master:parquet-column/src/test/java/org/apache/parquet/column/values/deltastrings/TestDeltaByteArray.java

public class TestDeltaByteArray {

  static String[] values = {"parquet-mr", "parquet", "parquet-format"};
  static String[] randvalues = Utils.getRandomStringSamples(10000, 32);

  @Test
<<<<<<< HEAD:parquet-column/src/test/java/parquet/column/values/deltastrings/TestDeltaByteArray.java
  public void testSerialization () throws IOException {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64*1024, new DirectByteBufferAllocator());
    DeltaByteArrayReader reader = new DeltaByteArrayReader();

    Utils.writeData(writer, values);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toByteArray(), values.length);

    for(int i =0; i< bin.length ; i++) {
      Assert.assertEquals(Binary.fromString(values[i]).toStringUsingUTF8(), bin[i].toStringUsingUTF8());
    }
=======
  public void testSerialization () throws Exception {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024);
    DeltaByteArrayReader reader = new DeltaByteArrayReader();

    assertReadWrite(writer, reader, values);
>>>>>>> master:parquet-column/src/test/java/org/apache/parquet/column/values/deltastrings/TestDeltaByteArray.java
  }

  @Test
<<<<<<< HEAD:parquet-column/src/test/java/parquet/column/values/deltastrings/TestDeltaByteArray.java
  public void testRandomStrings() throws IOException {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64*1024, new DirectByteBufferAllocator());
    DeltaByteArrayReader reader = new DeltaByteArrayReader();

    Utils.writeData(writer, randvalues);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toByteArray(), randvalues.length);

    for(int i =0; i< bin.length ; i++) {
      Assert.assertEquals(Binary.fromString(randvalues[i]).toStringUsingUTF8(), bin[i].toStringUsingUTF8());
    }
=======
  public void testRandomStrings() throws Exception {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024);
    DeltaByteArrayReader reader = new DeltaByteArrayReader();
    assertReadWrite(writer, reader, randvalues);
>>>>>>> master:parquet-column/src/test/java/org/apache/parquet/column/values/deltastrings/TestDeltaByteArray.java
  }

  @Test
  public void testLengths() throws IOException {
<<<<<<< HEAD:parquet-column/src/test/java/parquet/column/values/deltastrings/TestDeltaByteArray.java
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64*1024, new DirectByteBufferAllocator());
=======
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024);
>>>>>>> master:parquet-column/src/test/java/org/apache/parquet/column/values/deltastrings/TestDeltaByteArray.java
    ValuesReader reader = new DeltaBinaryPackingValuesReader();

    Utils.writeData(writer, values);
    byte[] data = writer.getBytes().toByteArray();
    int[] bin = Utils.readInts(reader, data, values.length);

    // test prefix lengths
    Assert.assertEquals(0, bin[0]);
    Assert.assertEquals(7, bin[1]);
    Assert.assertEquals(7, bin[2]);

    int offset = reader.getNextOffset();
    reader = new DeltaBinaryPackingValuesReader();
    bin = Utils.readInts(reader, writer.getBytes().toByteArray(), offset, values.length);
    // test suffix lengths
    Assert.assertEquals(10, bin[0]);
    Assert.assertEquals(0, bin[1]);
    Assert.assertEquals(7, bin[2]);
  }

  private void assertReadWrite(DeltaByteArrayWriter writer, DeltaByteArrayReader reader, String[] vals) throws Exception {
    Utils.writeData(writer, vals);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toByteArray(), vals.length);

    for(int i = 0; i< bin.length ; i++) {
      Assert.assertEquals(Binary.fromString(vals[i]), bin[i]);
    }
  }

  @Test
  public void testWriterReset() throws Exception {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024);

    assertReadWrite(writer, new DeltaByteArrayReader(), values);

    writer.reset();

    assertReadWrite(writer, new DeltaByteArrayReader(), values);
  }
}
