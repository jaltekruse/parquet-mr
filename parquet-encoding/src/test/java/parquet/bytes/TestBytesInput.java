/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package parquet.bytes;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class TestBytesInput {

  @Test
  public void testBytesInputBufferReuse() throws IOException {

    int length = 100;

    byte[] data = new byte[length];
    for (int i = 0; i < length; i++){
      data[i] = (byte) i;
    }
    ByteArrayOutputStream BAOS = new ByteArrayOutputStream(length);
    BAOS.write(data, 0, length);
    CapacityByteArrayOutputStream CBAOS = new CapacityByteArrayOutputStream(length);
    CBAOS.write(data, 0, length);
    byte[] partialBufferData = new byte[150];
    System.arraycopy(data, 0, partialBufferData, 50, length);
    BytesInput[] inputs = {
      BytesInput.from(data),
      BytesInput.from(BAOS),
      BytesInput.from(CBAOS),
      BytesInput.from(partialBufferData, 50, length)
    };

    byte[] dest;
    int offset = 30;
    for (BytesInput bi : inputs){
      dest = new byte[200];
      bi.writeTo(dest, offset, length);
      for (int i = 0; i < 100; i++){
        assert data[i] == dest[i + offset] : "something went wrong while copying data into a reused buffer";
      }
    }
  }
}
