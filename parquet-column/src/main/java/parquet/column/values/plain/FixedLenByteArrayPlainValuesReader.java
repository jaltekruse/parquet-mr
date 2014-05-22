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
package parquet.column.values.plain;

import java.io.IOException;
import java.nio.ByteBuffer;

import parquet.Log;
import parquet.column.values.ValuesReader;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;

import static parquet.Log.DEBUG;

/**
 * ValuesReader for FIXED_LEN_BYTE_ARRAY.
 *
 * @author David Z. Chen <dchen@linkedin.com>
 */
public class FixedLenByteArrayPlainValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(FixedLenByteArrayPlainValuesReader.class);
  private ByteBuffer in;
  private int offset;
  private int length;

  public FixedLenByteArrayPlainValuesReader(int length) {
    this.length = length;
  }

  @Override
  public Binary readBytes() {
    try {
      int start = offset;
      offset = start + length;
      return Binary.fromByteBuffer(in, start, length);
    } catch (RuntimeException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + offset, e);
    }
  }

  @Override
  public void skip() {
    offset += length;
  }

  @Override
  public void initFromPage(int valueCount, ByteBuffer in, int offset)
      throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.limit() - offset));
    this.in = in;
    this.offset = offset;
  }
  
  @Override
  public void initFromPage(int valueCount, byte[] page, int offset) throws IOException{
    this.initFromPage(valueCount, ByteBuffer.wrap(page), offset);
  }
}
