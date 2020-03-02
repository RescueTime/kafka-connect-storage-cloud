/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3.storage;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Vector;

@SuppressWarnings("RedundantThrows")
public class ChunkedDiskBuffer {
  private static final Logger log = LoggerFactory.getLogger(ChunkedDiskBuffer.class);

  String fileNameRoot;
  Vector<UploadPart> parts = new Vector<>();
  private int partSize;

  ChunkedDiskBuffer(String key, S3SinkConnectorConfig config) {
    this.partSize = config.getPartSize();
    fileNameRoot = config.getBufferTmpDir() + "/"
        + config.getBucketName().replaceAll("/", "-") + "-"
        + key.replaceAll("/", "-") + ".buffer";
    parts.add(new UploadPart(parts.size()));
  }

  void close() throws IOException {
    for (UploadPart part : parts) {
      part.close();
    }
  }

  UploadPart currentPart() {
    return parts.lastElement();
  }

  public void write(int b) throws IOException {
    if (currentPart().remaining() < 1) {
      parts.add(new UploadPart(parts.size()));
    }
    currentPart().outputStream.write((byte) b);
    currentPart().numBytesWritten++;
  }

  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (outOfRange(off, b.length) || len < 0 || outOfRange(off + len, b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    int totalWritten = 0;
    while (totalWritten < len) {
      if (currentPart().remaining() < len) {
        int firstPart = currentPart().remaining();
        currentPart().outputStream.write(b, off, firstPart);
        currentPart().numBytesWritten += firstPart;
        totalWritten += firstPart;
        // start a new part
        parts.add(new UploadPart(parts.size()));
      } else {
        currentPart().outputStream.write(b, off, len);
        currentPart().numBytesWritten += len;
        totalWritten += len;
      }
    }
  }

  private static boolean outOfRange(int off, int len) {
    return off < 0 || off > len;
  }

  List<UploadPart> getUploadParts() {
    return parts;
  }

  public class UploadPart {

    int partID;
    int numBytesWritten = 0;
    int numBytesRead = 0;
    private File bufferFile;
    OutputStream outputStream;

    UploadPart(int partID) {
      this.partID = partID;
      bufferFile = new java.io.File(filename());
      if (bufferFile.exists()) {
        boolean deleted = bufferFile.delete();
        log.debug("File {} already exists, deleted? {}", filename(), deleted);
      }

      try {
        if (bufferFile.createNewFile()) {
          this.outputStream = new FileOutputStream(bufferFile);
        } else {
          throw new RuntimeException("File could not be created: " + filename());
        }
      } catch (IOException ioe) {
        log.error("IOException opening file: " + filename());
        throw new RuntimeException(ioe);
      }
    }

    int remaining() {
      return partSize - numBytesWritten;
    }

    String filename() {
      return ChunkedDiskBuffer.this.fileNameRoot + "." + partID;
    }

    InputStream getInputStream() {
      try {
        return new FileInputStream(bufferFile);
      } catch (FileNotFoundException fnfe) {
        log.error("FileNotFoundException getting inputStream: " + filename());
        throw new RuntimeException(fnfe);
      }
    }

    void rewind() {
      try {
        outputStream.close();
      } catch (IOException ioe) {
        log.error("IOException closing outputStream: " + filename());
        throw new RuntimeException(ioe);
      }
      numBytesRead = 0;
    }

    public void close() throws IOException {
      rewind();
      if (bufferFile != null) {
        boolean deleted = bufferFile.delete();
        log.debug("closing buffer file {}, deleted? {}", filename(), deleted);
      }
    }
  }

}
