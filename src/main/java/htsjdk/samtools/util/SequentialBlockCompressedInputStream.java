/*
 */
package htsjdk.samtools.util;


import htsjdk.samtools.FileTruncatedException;
import htsjdk.samtools.seekablestream.SeekableBufferedStream;
import htsjdk.samtools.seekablestream.SeekableFileStream;
import htsjdk.samtools.seekablestream.SeekableHTTPStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.zip.InflaterFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.*;

import static htsjdk.samtools.util.BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE;

/**
 */
public class SequentialBlockCompressedInputStream extends BlockCompressedInputStream {

    private final SequentialReadingProducer readingProducer;

    private boolean isEndOfReading = false;


    public SequentialBlockCompressedInputStream(final InputStream stream) {
        super(stream, true);

        readingProducer = SequentialReadingProducer.makeProducerForInputStream(IOUtil.toBufferedStream(stream),
                BlockGunzipper.getDefaultInflaterFactory());
    }

    public SequentialBlockCompressedInputStream(final InputStream stream, final boolean allowBuffering) {
        super(stream, allowBuffering);

        readingProducer = SequentialReadingProducer.makeProducerForInputStream(allowBuffering ? IOUtil.toBufferedStream(stream) : stream,
                BlockGunzipper.getDefaultInflaterFactory());
    }

    public SequentialBlockCompressedInputStream(final InputStream stream, final boolean allowBuffering, InflaterFactory inflaterFactory) {
        super(stream, allowBuffering, inflaterFactory);

        readingProducer = SequentialReadingProducer.makeProducerForInputStream(allowBuffering ? IOUtil.toBufferedStream(stream) : stream,
                inflaterFactory);
    }

    public SequentialBlockCompressedInputStream(final InputStream stream, InflaterFactory inflaterFactory) {
        super(stream, true, inflaterFactory);

        readingProducer = SequentialReadingProducer.makeProducerForInputStream(IOUtil.toBufferedStream(stream), inflaterFactory);
    }

    public SequentialBlockCompressedInputStream(final File file)
        throws IOException {
        super(file);

        readingProducer = SequentialReadingProducer.makeProducerForSeekableStream(new SeekableFileStream(file), BlockGunzipper.getDefaultInflaterFactory());
    }

    public SequentialBlockCompressedInputStream(final File file, InflaterFactory inflaterFactory)
            throws IOException {
        super(file, inflaterFactory);

        readingProducer = SequentialReadingProducer.makeProducerForSeekableStream(new SeekableFileStream(file), inflaterFactory);
    }

    public SequentialBlockCompressedInputStream(final URL url) {
        super(url);
        readingProducer = SequentialReadingProducer.makeProducerForSeekableStream(new SeekableBufferedStream(new SeekableHTTPStream(url)), BlockGunzipper.getDefaultInflaterFactory());
    }

    public SequentialBlockCompressedInputStream(final URL url, InflaterFactory inflaterFactory) {
        super(url, inflaterFactory);
        readingProducer = SequentialReadingProducer.makeProducerForSeekableStream(new SeekableBufferedStream(new SeekableHTTPStream(url)), inflaterFactory);
    }

    public SequentialBlockCompressedInputStream(final SeekableStream strm) {
        super(strm);
        readingProducer = SequentialReadingProducer.makeProducerForSeekableStream(strm, BlockGunzipper.getDefaultInflaterFactory());
    }

    public SequentialBlockCompressedInputStream(final SeekableStream strm, InflaterFactory inflaterFactory) {
        super(strm, inflaterFactory);
        readingProducer = SequentialReadingProducer.makeProducerForSeekableStream(strm, inflaterFactory);
    }

    @Override
    protected DecompressedBlock readNextBlock() {
        if (isEndOfReading || isLastCurrentBlock()) {
            isEndOfReading = true;
            return null;
        }
        return nextBlockAsync();
    }
    
    @Override
    protected void prepareForSeek() {
        throw new RuntimeException(CANNOT_SEEK_STREAM_MSG);
    }

    @Override
    public void close() throws IOException {
        try {
            readingProducer.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        super.close();
    }

    private DecompressedBlock nextBlockAsync() {
        try {
            return readingProducer.take();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static class SequentialReadingProducer {

        private final ExecutorService readerExecutor;
        private final ReadingTask producerTask;
        private final int QUEUE_CAPACITY = Runtime.getRuntime().availableProcessors() * 2;
        private final int UNZIP_THREADS = Runtime.getRuntime().availableProcessors();
        private final int NUMBER_OF_BUFFERS = QUEUE_CAPACITY + UNZIP_THREADS * 2 + 2;
        private final BlockingQueue<Future<DecompressedBlock>> resultQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        public static SequentialReadingProducer makeProducerForSeekableStream(SeekableStream file, final InflaterFactory inflaterFactory) {
            return new SequentialReadingProducer(file, inflaterFactory);
        }

        public static SequentialReadingProducer makeProducerForInputStream(InputStream stream, final InflaterFactory inflaterFactory) {
            return new SequentialReadingProducer(inflaterFactory, stream);
        }

        private SequentialReadingProducer(final SeekableStream file, final InflaterFactory inflaterFactory) {

            if (file == null) {
                throw new IllegalArgumentException("Input file is null");
            }

            readerExecutor = Executors.newSingleThreadExecutor();
            producerTask = new ReadingTask(null, file, inflaterFactory, resultQueue, file.getSource(), NUMBER_OF_BUFFERS, UNZIP_THREADS);
            readerExecutor.execute(producerTask);
            readerExecutor.shutdown();
        }

        private SequentialReadingProducer(final InflaterFactory inflaterFactory, final InputStream stream) {

            if (stream == null) {
                throw new IllegalArgumentException("InputStream is null");
            }

            readerExecutor = Executors.newSingleThreadExecutor();
            producerTask = new ReadingTask(stream, null, inflaterFactory, resultQueue, "data stream", NUMBER_OF_BUFFERS, UNZIP_THREADS);
            readerExecutor.execute(producerTask);
            readerExecutor.shutdown();

        }

        public void close() throws IOException, InterruptedException {
            producerTask.close();
            readerExecutor.shutdownNow();
        }

        public DecompressedBlock take() throws InterruptedException, ExecutionException {
            return resultQueue.take().get();
        }

        private static class ReadingTask implements Runnable {

            private final BlockingQueue<Future<DecompressedBlock>> queue;
            private final ExecutorService unzipExecutor;

            private long mOffset = 0;
            private final InputStream pStream;
            private final SeekableStream pFile;
            private final String source;
            private final int nBuffers;
            private int currentNumberBuffer = 0;
            private ProcessBuffer[] processBuffers;

            private ReadingTask(final InputStream stream, SeekableStream mFile,
                                final InflaterFactory inflaterFactory,
                                BlockingQueue<Future<DecompressedBlock>> queue,
                                String source,
                                int nBuffers,
                                int nUnzipThread) {
                this.queue = queue;
                this.pStream = IOUtil.toBufferedStream(stream);
                this.pFile = mFile;
                this.source = source;
                this.nBuffers = nBuffers;
                this.processBuffers = makeTempBuffers(this.nBuffers, inflaterFactory);

                unzipExecutor = Executors.newFixedThreadPool(nUnzipThread);
            }

            public void close() throws IOException, InterruptedException {
                if (pFile != null) {
                    pFile.close();
                }

                unzipExecutor.shutdown();
            }

            private ProcessBuffer[] makeTempBuffers(int nBuffers, final InflaterFactory inflaterFactory) {
                ProcessBuffer[] buffers = new ProcessBuffer[nBuffers];
                for (int i = 0; i < nBuffers; i++) {
                    ProcessBuffer buffer = new ProcessBuffer(
                            new byte[MAX_COMPRESSED_BLOCK_SIZE],
                            new byte[MAX_COMPRESSED_BLOCK_SIZE],
                            new BlockGunzipper(inflaterFactory));
                    buffers[i] = buffer;
                }
                return buffers;
            }

            private ProcessBuffer getNextBuffer() {
                int num = currentNumberBuffer++ % nBuffers;
                return this.processBuffers[num];
            }

            private int readBytes(final byte[] buffer, final int offset, final int length) throws IOException {
                return BlockCompressedInputStream.readBytes(pStream, pFile, buffer, offset, length);
            }

            private ReadingResultBuffer processNextBlockForAsync(ProcessBuffer processBuffers) {
                long blockAddress = mOffset;
                try {
                    final int headerByteCount = readBytes(processBuffers.readBuffer, 0, BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH);
                    mOffset += headerByteCount;
                    if (headerByteCount == 0) {
                        // Handle case where there is no empty gzip block at end.
                        return new ReadingResultBuffer(new DecompressedBlock(blockAddress, new byte[0], 0));
                    }
                    if (headerByteCount != BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH) {
                        return new ReadingResultBuffer(new DecompressedBlock(blockAddress, headerByteCount, new IOException(INCORRECT_HEADER_SIZE_MSG + source)));
                    }
                    final int blockLength = unpackInt16(processBuffers.readBuffer, BlockCompressedStreamConstants.BLOCK_LENGTH_OFFSET) + 1;
                    if (blockLength < BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH || blockLength > processBuffers.readBuffer.length) {
                        return new ReadingResultBuffer(new DecompressedBlock(blockAddress, blockLength,
                                new IOException(UNEXPECTED_BLOCK_LENGTH_MSG + blockLength + " for " + source)));
                    }
                    final int remaining = blockLength - BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH;
                    final int dataByteCount = readBytes(processBuffers.readBuffer, BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH,
                            remaining);
                    mOffset += dataByteCount;
                    if (dataByteCount != remaining) {
                        return new ReadingResultBuffer(new DecompressedBlock(blockAddress, blockLength,
                                new FileTruncatedException(PREMATURE_END_MSG + source)));
                    }
                    return new ReadingResultBuffer(processBuffers, blockAddress, blockLength);
                } catch (IOException e) {
                    return new ReadingResultBuffer(new DecompressedBlock(blockAddress, 0, e));
                }
            }

            private DecompressedBlock unzip(ReadingResultBuffer readingResult) {
                try {
                    ProcessBuffer processBuffer = readingResult.processBuffer;
                    final byte[] decompressed = inflateBlock(processBuffer.blockGunzipper, processBuffer.readBuffer, readingResult.blockLength,
                            processBuffer.decompressedBuffer, source);

                    return new DecompressedBlock(readingResult.blockAddress, decompressed, readingResult.blockLength);
                } catch (IOException e) {
                    return new DecompressedBlock(readingResult.blockAddress, 0, e);
                }
            }

            private boolean isEndBlock(ReadingResultBuffer readingResultBuffer) {
                if (readingResultBuffer.getBlock() == null) {
                    return false;
                }

                if (readingResultBuffer.getBlock().getCompressedSize() == 0
                        || readingResultBuffer.getBlock().isException()) {
                    return true;
                }

                return false;

            }

            @Override
            public void run() {
                boolean isNotInterrupted = true;
                while (isNotInterrupted) {
                    try {
                        ReadingResultBuffer readingResultBuffer = processNextBlockForAsync(getNextBuffer());
                        boolean isLastBlock = isEndBlock(readingResultBuffer);

                        if (readingResultBuffer.processBuffer != null) {
                            queue.put(unzipExecutor.submit(() -> unzip(readingResultBuffer)));
                        } else {
                            queue.put(unzipExecutor.submit(readingResultBuffer::getBlock));
                        }

                        if (isLastBlock) {
                            unzipExecutor.shutdown();
                            break;
                        }
                    } catch (InterruptedException | RejectedExecutionException e) {
                        isNotInterrupted = false;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            private static class ProcessBuffer {
                private final byte[] readBuffer;
                private final byte[] decompressedBuffer;
                private final BlockGunzipper blockGunzipper;

                public ProcessBuffer(byte[] readBuffer, byte[] decompressedBuffer,
                                     BlockGunzipper blockGunzipper) {
                    this.readBuffer = readBuffer;
                    this.decompressedBuffer = decompressedBuffer;
                    this.blockGunzipper = blockGunzipper;
                }
            }

            private static class ReadingResultBuffer {
                private final DecompressedBlock block;
                private final ProcessBuffer processBuffer;
                private final long blockAddress;
                private final int blockLength;

                public ReadingResultBuffer(ProcessBuffer processBuffer, long blockAddress, int blockLength) {
                    this.block = null;
                    this.processBuffer = processBuffer;
                    this.blockLength = blockLength;
                    this.blockAddress = blockAddress;
                }

                public ReadingResultBuffer(DecompressedBlock block) {
                    this.block = block;
                    this.processBuffer = null;
                    this.blockAddress = 0;
                    this.blockLength = 0;
                }

                public DecompressedBlock getBlock() {
                    return this.block;
                }
            }
        }
    }
}
