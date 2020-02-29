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
import sun.misc.Cleaner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.SequenceInputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Vector;

@SuppressWarnings("RedundantThrows")
public class ChunkedDiskBuffer {
  private static final Logger log = LoggerFactory.getLogger(ChunkedDiskBuffer.class);

  private String fileNameRoot;
  Vector<UploadPart> parts = new Vector<>();
  private int partSize;
  private int chunkSize = 1024 * 10;  // TODO configurable?

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
    MappedByteBuffer currentBuffer = currentPart().currentBuffer();
    if (!currentBuffer.hasRemaining()) {
      if (currentPart().numBytesWritten + 1 >= partSize) {
        parts.add(new UploadPart(parts.size()));
        currentBuffer = currentPart().currentBuffer();
      } else {
        currentPart().addChunk();
        currentBuffer = currentPart().currentBuffer();
      }
    }
    currentBuffer.put((byte) b);
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
      MappedByteBuffer currentBuffer = currentPart().currentBuffer();
      if (currentBuffer.remaining() < len) {
        int firstPart = currentBuffer.remaining();
        currentBuffer.put(b, off, firstPart);
        currentPart().numBytesWritten += firstPart;
        totalWritten += firstPart;
        if (currentPart().numBytesWritten >= partSize) {
          // start a new part
          parts.add(new UploadPart(parts.size()));
        } else {
          // start a new chunk in current part
          currentPart().addChunk();
        }
      } else {
        currentBuffer.put(b, off, len);
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

    int streamID;
    int currentChunk = -1;
    Vector<Chunk> chunks = new Vector<>();
    int numBytesWritten = 0;
    int numBytesRead = 0;

    UploadPart(int streamID) {
      this.streamID = streamID;
      addChunk();
      currentBuffer().rewind();
    }

    InputStream getInputStream() {
      // SequenceInputStream concatenates all of our constituent chunks into 1 continuous stream
      return new SequenceInputStream(chunks.elements());
    }

    void rewind() {
      currentChunk = 0;
      numBytesRead = 0;
      for (Chunk c : chunks) {
        c.buffer.rewind();
      }
    }

    Chunk currentChunk() {
      return chunks.lastElement();
    }

    MappedByteBuffer currentBuffer() {
      return currentChunk().buffer;
    }

    void addChunk() {
      chunks.add(new Chunk(streamID, chunks.size()));
      currentChunk++;
    }

    public void close() throws IOException {
      for (Chunk c : chunks) {
        c.close();
      }
    }

    // adapted from https://stackoverflow.com/questions/4332264/wrapping-a-bytebuffer-with-an-inputstream
    public class Chunk extends InputStream {
      private int partID;
      private int id;
      MappedByteBuffer buffer;
      private File bufferFile;
      private RandomAccessFile bufferRandomAccessFile;
      private FileChannel bufferFileChannel;

      Chunk(int partID, int id) {
        // https://www.tothenew.com/blog/handling-large-files-using-javanio-mappedbytebuffer/
        // https://howtodoinjava.com/java7/nio/memory-mapped-files-mappedbytebuffer/
        this.partID = partID;
        this.id = id;
        log.debug("Trying to init new MappedByteBuffer Chunk on {}", filename());
        try {
          bufferFile = new java.io.File(filename());
          if (bufferFile.exists()) {
            boolean deleted = bufferFile.delete();
            log.debug("File {} already exists, deleted? {}", filename(), deleted);
          }
          if (bufferFile.createNewFile()) {
            bufferRandomAccessFile = new RandomAccessFile(bufferFile, "rw");
            bufferFileChannel = bufferRandomAccessFile.getChannel();
            this.buffer = bufferFileChannel.map(
                FileChannel.MapMode.READ_WRITE, 0, chunkSize);
          } else {
            throw new RuntimeException("File could not be created: " + filename());
          }
        } catch (FileNotFoundException fnfe) {
          log.error("FileNotFoundException opening file: " + filename());
          throw new RuntimeException(fnfe);
        } catch (IOException ioe) {
          log.error("IOException opening file: " + filename());
          throw new RuntimeException(ioe);
        }
        log.debug("MappedByteBuffer Chunk was successfully initialized on {}", filename());
      }

      String filename() {
        return ChunkedDiskBuffer.this.fileNameRoot + "." + partID + "." + id;
      }

      @Override
      public void close() throws IOException {
        if (buffer == null) {
          return;
        }
        try {
          buffer.clear();
          // make sure JVM deallocates mapped buffer so we can delete file
          Cleaner cleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();
          if (cleaner != null) {
            cleaner.clean();
          }
        } finally {
          if (bufferFileChannel != null) {
            bufferFileChannel.close();
          }
          if (bufferRandomAccessFile != null) {
            bufferRandomAccessFile.close();
          }
          if (bufferFile != null) {
            boolean deleted = bufferFile.delete();
            log.debug("Closing chunk {}, deleted? {}", filename(), deleted);
          }
          bufferFile = null;
          bufferRandomAccessFile = null;
          bufferFileChannel = null;
          buffer = null;
        }
      }

      public int read() throws IOException {
        if (!buffer.hasRemaining()) {
          return -1;
        }
        return buffer.get() & 0xFF;
      }

      @SuppressWarnings("NullableProblems")
      @Override
      public int read(byte[] bytes, int off, int len) throws IOException {
        int numToRead = Math.min(len, buffer.remaining());
        if (numToRead <= 0) {
          return -1;
        }
        buffer.get(bytes, off, numToRead);
        return numToRead;
      }

    }

  }

}
