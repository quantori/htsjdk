package htsjdk.samtools.util.output.async;

import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.samtools.util.IOUtil;
import htsjdk.samtools.util.RuntimeIOException;
import htsjdk.samtools.util.output.AbstractBlockCompressedOutputStream;
import htsjdk.samtools.util.zip.DeflaterFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

public class AsyncBlockCompressedOutputStream extends AbstractBlockCompressedOutputStream {

    private static final int COMPRESSING_THREADS = countCompressingThread();

    private static final ExecutorService gzipExecutorService = Executors.newFixedThreadPool(COMPRESSING_THREADS, runnable -> {
        Thread thread = Executors.defaultThreadFactory().newThread(runnable);
        thread.setDaemon(true);
        return thread;
    });

    private static final Void NOTHING = null;
    private static final int BLOCKS_PACK_SIZE = 16 * COMPRESSING_THREADS;

    private DeflaterFactory deflaterFactory;
    private final int compressionLevel;

    private List<Future<CompressedBlock>> compressedBlocksInFuture = new ArrayList<>(BLOCKS_PACK_SIZE);
    private CompletableFuture<Void> writeBlocksTask = CompletableFuture.completedFuture(NOTHING);

    /**
     * Uses default compression level, which is 5 unless changed by setCompressionLevel
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * Use {@link #AsyncBlockCompressedOutputStream(File, int, DeflaterFactory)} to specify a custom factory.
     */
    public AsyncBlockCompressedOutputStream(final String filename) {
        this(filename, defaultCompressionLevel);
    }

    /**
     * Uses default compression level, which is 5 unless changed by setCompressionLevel
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * Use {@link #AsyncBlockCompressedOutputStream(File, int, DeflaterFactory)} to specify a custom factory.
     */
    public AsyncBlockCompressedOutputStream(final File file) {
        this(file, defaultCompressionLevel);
    }

    /**
     * Prepare to compress at the given compression level
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     *
     * @param compressionLevel 1 <= compressionLevel <= 9
     */
    public AsyncBlockCompressedOutputStream(final String filename, final int compressionLevel) {
        this(new File(filename), compressionLevel);
    }

    /**
     * Prepare to compress at the given compression level
     *
     * @param compressionLevel 1 <= compressionLevel <= 9
     *                         Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     *                         Use {@link #AsyncBlockCompressedOutputStream(File, int, DeflaterFactory)} to specify a custom factory.
     */
    public AsyncBlockCompressedOutputStream(final File file, final int compressionLevel) {
        this(file, compressionLevel, defaultDeflaterFactory);
    }

    /**
     * Prepare to compress at the given compression level
     *
     * @param compressionLevel 1 <= compressionLevel <= 9
     * @param deflaterFactory  custom factory to create deflaters (overrides the default)
     */
    public AsyncBlockCompressedOutputStream(final File file, final int compressionLevel, final DeflaterFactory deflaterFactory) {
        this(IOUtil.toPath(file), compressionLevel, deflaterFactory);
    }

    public AsyncBlockCompressedOutputStream(final OutputStream os, final Path file) {
        super(os, file);
        this.compressionLevel = defaultCompressionLevel;
        this.deflaterFactory = defaultDeflaterFactory;
    }

    /**
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * Use {@link #AsyncBlockCompressedOutputStream(OutputStream, Path, int, DeflaterFactory)} to specify a custom factory.
     */
    public AsyncBlockCompressedOutputStream(final OutputStream os, final Path file, final int compressionLevel) {
        this(os, file, compressionLevel, defaultDeflaterFactory);
    }

    /**
     * Prepare to compress at the given compression level
     *
     * @param compressionLevel 1 <= compressionLevel <= 9
     * @param deflaterFactory  custom factory to create deflaters (overrides the default)
     */
    public AsyncBlockCompressedOutputStream(final Path path, final int compressionLevel, final DeflaterFactory deflaterFactory) {
        super(path);
        this.compressionLevel = compressionLevel;
        this.deflaterFactory = deflaterFactory;
    }

    /**
     * Creates the output stream.
     *
     * @param os               output stream to create a BlockCompressedOutputStream from
     * @param file             file to which to write the output or null if not available
     * @param compressionLevel the compression level (0-9)
     * @param deflaterFactory  custom factory to create deflaters (overrides the default)
     */
    public AsyncBlockCompressedOutputStream(final OutputStream os, final Path file, final int compressionLevel, final DeflaterFactory deflaterFactory) {
        super(os, file);
        this.compressionLevel = compressionLevel;
        this.deflaterFactory = deflaterFactory;
    }

    private static int countCompressingThread() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        return (availableProcessors <= 2) ? 2 : (availableProcessors - 2);
    }

    /**
     * WARNING: flush() affects the output format, because it causes the current contents of uncompressedBuffer
     * to be compressed and written, even if it isn't full.  Unless you know what you're doing, don't call flush().
     * Instead, call close(), which will flush any unwritten data before closing the underlying stream.
     */
    @Override
    public void flush() throws IOException {
        while (numUncompressedBytes > 0) {
            deflateBlock();
        }
        processWritingTask();
        writeBlocksTask.join();
        codec.getOutputStream().flush();
    }

    /**
     * close() must be called in order to flush any remaining buffered bytes. An unclosed file will likely be
     * defective.
     */
    @Override
    public void close() throws IOException {
        close(true);
    }

    /**
     * @see AbstractBlockCompressedOutputStream#getFilePointer();
     * This implementation of get file pointer is blocking, because we have to wait to the end
     * of writing all provided data for getting right pointer.
     */
    @Override
    public long getFilePointer() {
        processWritingTask();
        writeBlocksTask.join();
        return super.getFilePointer();
    }

    /**
     * Process new deflate task of the current uncompressed byte buffer.
     */
    @Override
    protected void deflateBlock() {
        if (numUncompressedBytes == 0) {
            return;
        }
        processDeflateTask();
        numUncompressedBytes = 0;

        if (BLOCKS_PACK_SIZE == compressedBlocksInFuture.size()) {
            processWritingTask();
        }
    }

    void processDeflateTask() {
        UncompressedBlock uncompressedBlock =
                new UncompressedBlock(Arrays.copyOf(this.uncompressedBuffer, numUncompressedBytes), numUncompressedBytes);
        compressedBlocksInFuture.add(CompletableFuture.supplyAsync(new BlockCompressingTask(uncompressedBlock), gzipExecutorService));
    }

    void processWritingTask() {
        writeBlocksTask.join();
        writeBlocksTask = CompletableFuture.runAsync(new BlockWritingTask(compressedBlocksInFuture), gzipExecutorService);
        compressedBlocksInFuture = new ArrayList<>(BLOCKS_PACK_SIZE);
    }

    private class BlockWritingTask implements Runnable {

        private List<Future<CompressedBlock>> compressedBlocks;

        private BlockWritingTask(List<Future<CompressedBlock>> compressedBlocks) {
            this.compressedBlocks = compressedBlocks;
        }

        @Override
        public void run() {
            for (Future<CompressedBlock> compressedBlockFuture : compressedBlocks) {
                try {
                    CompressedBlock compressedBlock = compressedBlockFuture.get();
                    mBlockAddress += writeGzipBlock(compressedBlock.getCompressedBuffer(), compressedBlock.getCompressedSize(),
                            compressedBlock.getBytesToCompress(), compressedBlock.getCrc32Value());

                    // Call out to the indexer if it exists
                    if (indexer != null) {
                        indexer.addGzipBlock(mBlockAddress, compressedBlock.getBytesToCompress());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeIOException("Enable to write Compressed block", e);
                }
            }
        }
    }

    private class BlockCompressingTask implements Supplier<CompressedBlock> {

        private UncompressedBlock uncompressedBlock;

        private BlockCompressingTask(UncompressedBlock uncompressedBlock) {
            this.uncompressedBlock = uncompressedBlock;
        }

        @Override
        public CompressedBlock get() {
            return compressBlock(uncompressedBlock);
        }

        private CompressedBlock compressBlock(UncompressedBlock uncompressedBlock) {
            final CRC32 crc32 = new CRC32();
            final Deflater deflater = deflaterFactory.makeDeflater(compressionLevel, true);

            final byte[] compressedBuffer = new byte[BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE -
                    BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH];
            // Compress the input
            deflater.setInput(uncompressedBlock.getUncompressedBuffer(), 0, uncompressedBlock.getBytesToCompress());
            deflater.finish();
            int compressedSize = deflater.deflate(compressedBuffer, 0, compressedBuffer.length);

            // If it didn't all fit in uncompressedBuffer.length, set compression level to NO_COMPRESSION
            // and try again.  This should always fit.
            if (!deflater.finished()) {
                final Deflater noCompressionDeflater = deflaterFactory.makeDeflater(Deflater.NO_COMPRESSION, true);
                noCompressionDeflater.setInput(uncompressedBlock.getUncompressedBuffer(), 0, uncompressedBlock.getBytesToCompress());
                noCompressionDeflater.finish();
                compressedSize = noCompressionDeflater.deflate(compressedBuffer, 0, compressedBuffer.length);
                if (!noCompressionDeflater.finished()) {
                    throw new IllegalStateException("Impossible");
                }
            }

            // Data compressed small enough, so write it out.
            crc32.update(uncompressedBlock.getUncompressedBuffer(), 0, uncompressedBlock.getBytesToCompress());
            return new CompressedBlock(compressedBuffer, compressedSize, uncompressedBlock.getBytesToCompress(), crc32.getValue());
        }
    }

    private static class CompressedBlock {

        private byte[] compressedBuffer;
        private int compressedSize;
        private int bytesToCompress;
        private long crc32Value;

        private CompressedBlock(byte[] compressedBuffer, int compressedSize, int bytesToCompress, long crc32Value) {
            this.compressedBuffer = compressedBuffer;
            this.compressedSize = compressedSize;
            this.bytesToCompress = bytesToCompress;
            this.crc32Value = crc32Value;
        }

        private byte[] getCompressedBuffer() {
            return compressedBuffer;
        }

        private int getCompressedSize() {
            return compressedSize;
        }

        private int getBytesToCompress() {
            return bytesToCompress;
        }

        private long getCrc32Value() {
            return crc32Value;
        }
    }

    private static class UncompressedBlock {

        private byte[] uncompressedBuffer;
        private int bytesToCompress;

        private UncompressedBlock(byte[] uncompressedBuffer, int bytesToCompress) {
            this.uncompressedBuffer = uncompressedBuffer;
            this.bytesToCompress = bytesToCompress;
        }

        private byte[] getUncompressedBuffer() {
            return uncompressedBuffer;
        }

        private int getBytesToCompress() {
            return bytesToCompress;
        }
    }

}
