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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("RedundantThrows")
public class ChunkedDiskBuffer {
  private static final Logger log = LoggerFactory.getLogger(ChunkedDiskBuffer.class);

  private String fileNameRoot;
  List<ByteBufferBackedInputStream> streams = new ArrayList<>();
  private int partSize;

  ChunkedDiskBuffer(String bucket, String key, int partSize) {
    this.partSize = partSize;
    // TODO configurable location for tmp
    fileNameRoot = "/tmp/"
        + bucket.replaceAll("/", "-") + "-"
        + key.replaceAll("/", "-") + ".buffer";
    streams.add(new ByteBufferBackedInputStream(streams.size()));
  }

  void close() throws IOException {
    for (ByteBufferBackedInputStream stream : streams) {
      stream.close();
    }
  }

  public void write(int b) throws IOException {
    ByteBufferBackedInputStream currentStream = streams.get(streams.size() - 1);
    MappedByteBuffer currentBuffer = currentStream.buffer;
    currentBuffer.put((byte) b);
    currentStream.numBytesWritten++;
    if (!currentBuffer.hasRemaining()) {
      // start a new stream
      streams.add(new ByteBufferBackedInputStream(streams.size()));
    }
  }

  public void write(byte[] b, int off, int len) throws IOException {
    log.info("Writing an array of bytes");
    if (b == null) {
      throw new NullPointerException();
    } else if (outOfRange(off, b.length) || len < 0 || outOfRange(off + len, b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    ByteBufferBackedInputStream currentStream = streams.get(streams.size() - 1);
    MappedByteBuffer currentBuffer = currentStream.buffer;

    if (currentBuffer.remaining() < len) {
      int firstPart = currentBuffer.remaining();
      currentBuffer.put(b, off, firstPart);
      currentStream.numBytesWritten += firstPart;
      streams.add(new ByteBufferBackedInputStream(streams.size()));
      write(b, off + firstPart, len - firstPart);
    } else {
      currentBuffer.put(b, off, len);
      currentStream.numBytesWritten += len;
    }
  }

  private static boolean outOfRange(int off, int len) {
    return off < 0 || off > len;
  }

  List<ByteBufferBackedInputStream> getInputStreams() {
    return streams;
  }

  // adapted from https://stackoverflow.com/questions/4332264/wrapping-a-bytebuffer-with-an-inputstream
  @SuppressWarnings({"RedundantThrows", "NullableProblems"})
  public class ByteBufferBackedInputStream extends InputStream {

    int streamID;
    MappedByteBuffer buffer;
    private File bufferFile;
    private RandomAccessFile bufferRandomAccessFile;
    private FileChannel bufferFileChannel;
    int numBytesWritten = 0;
    int numBytesRead = 0;

    ByteBufferBackedInputStream(int streamID) {
      this.streamID = streamID;
      log.info("Trying to init new MappedByteBuffer Chunk on {}", filename());
      try {
        bufferFile = new java.io.File(filename());
        if (bufferFile.exists()) {
          boolean deleted = bufferFile.delete();
          log.info("File {} already exists, deleted? {}", filename(), deleted);
        }
        if (bufferFile.createNewFile()) {
          bufferRandomAccessFile = new RandomAccessFile(bufferFile, "rw");
          bufferFileChannel = bufferRandomAccessFile.getChannel();
          this.buffer = bufferFileChannel.map(
              FileChannel.MapMode.READ_WRITE, 0, partSize);
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
      log.info("MappedByteBuffer Chunk was successfully initialized");

      buffer.rewind();
    }

    void rewind() {
      numBytesRead = 0;
      buffer.rewind();
    }

    String filename() {
      return ChunkedDiskBuffer.this.fileNameRoot + "." + streamID;
    }

    @Override
    public void close() throws IOException {
      super.close();
      try {
        buffer.clear();
        // make sure JVM deallocates mapped buffer so we can delete file
        Cleaner cleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();
        if (cleaner != null) {
          cleaner.clean();
        }
      } finally {
        bufferFileChannel.close();
        bufferRandomAccessFile.close();
        boolean deleted = bufferFile.delete();
        log.info("Attempt to delete file {} successful? {}", filename(), deleted);
        bufferFile = null;
        bufferRandomAccessFile = null;
        bufferFileChannel = null;
        buffer = null;
      }
    }

    private boolean isBufferAtEnd() {
      try {
        if (!buffer.hasRemaining()) {
          return true;
        }
      } catch (IndexOutOfBoundsException e) {
        return true;
      }
      return false;
    }

    public int read() throws IOException {
      if (isBufferAtEnd()) {
        return -1;
      }
      return buffer.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len) throws IOException {
      if (off == 0) {
        rewind();
      }
      if (isBufferAtEnd()) {
        return -1;
      }
      int numRead = Math.min(len, buffer.remaining());
      buffer.get(bytes, off, numRead);
      return numRead;
    }

  }
}
