/*
 * The MIT License
 *
 * Copyright (c) 2009 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package htsjdk.samtools.util;

import htsjdk.samtools.util.output.AbstractBlockCompressedOutputStream;
import htsjdk.samtools.util.zip.DeflaterFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

/**
 * Writer for a file that is a series of gzip blocks (BGZF format).  The caller just treats it as an
 * OutputStream, and under the covers a gzip block is written when the amount of uncompressed as-yet-unwritten
 * bytes reaches a threshold.
 *
 * The advantage of BGZF over conventional gzip is that BGZF allows for seeking without having to scan through
 * the entire file up to the position being sought.
 *
 * Note that the flush() method should not be called by client
 * unless you know what you're doing, because it forces a gzip block to be written even if the
 * number of buffered bytes has not reached threshold.  close(), on the other hand, must be called
 * when done writing in order to force the last gzip block to be written.
 *
 * c.f. http://samtools.sourceforge.net/SAM1.pdf for details of BGZF file format.
 */
public class BlockCompressedOutputStream extends AbstractBlockCompressedOutputStream
{

    private static final Log log = Log.getInstance(BlockCompressedOutputStream.class);

    private final byte[] compressedBuffer =
            new byte[BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE -
                    BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH];
    private final Deflater deflater;

    // A second deflater is created for the very unlikely case where the regular deflation actually makes
    // things bigger, and the compressed block is too big.  It should be possible to downshift the
    // primary deflater to NO_COMPRESSION level, recompress, and then restore it to its original setting,
    // but in practice that doesn't work.
    // The motivation for deflating at NO_COMPRESSION level is that it will predictably produce compressed
    // output that is 10 bytes larger than the input, and the threshold at which a block is generated is such that
    // the size of tbe final gzip block will always be <= 64K.  This is preferred over the previous method,
    // which would attempt to compress up to 64K bytes, and if the resulting compressed block was too large,
    // try compressing fewer input bytes (aka "downshifting').  The problem with downshifting is that
    // getFilePointer might return an inaccurate value.
    // I assume (AW 29-Oct-2013) that there is no value in using hardware-assisted deflater for no-compression mode,
    // so just use JDK standard.
    private final Deflater noCompressionDeflater = new Deflater(Deflater.NO_COMPRESSION, true);
    private final CRC32 crc32 = new CRC32();

    /**
     * Uses default compression level, which is 5 unless changed by setCompressionLevel
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * Use {@link #BlockCompressedOutputStream(File, int, DeflaterFactory)} to specify a custom factory.
     */
    public BlockCompressedOutputStream(final String filename) {
        this(filename, defaultCompressionLevel);
    }

    /**
     * Uses default compression level, which is 5 unless changed by setCompressionLevel
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * Use {@link #BlockCompressedOutputStream(File, int, DeflaterFactory)} to specify a custom factory.
     */
    public BlockCompressedOutputStream(final File file) {
        this(file, defaultCompressionLevel);
    }

    /**
     * Prepare to compress at the given compression level
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * @param compressionLevel 1 <= compressionLevel <= 9
     */
    public BlockCompressedOutputStream(final String filename, final int compressionLevel) {
        this(new File(filename), compressionLevel);
    }

    /**
     * Prepare to compress at the given compression level
     * @param compressionLevel 1 <= compressionLevel <= 9
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * Use {@link #BlockCompressedOutputStream(File, int, DeflaterFactory)} to specify a custom factory.
     */
    public BlockCompressedOutputStream(final File file, final int compressionLevel) {
        this(file, compressionLevel, defaultDeflaterFactory);
    }

    /**
     * Prepare to compress at the given compression level
     * @param compressionLevel 1 <= compressionLevel <= 9
     * @param deflaterFactory custom factory to create deflaters (overrides the default)
     */
    public BlockCompressedOutputStream(final File file, final int compressionLevel, final DeflaterFactory deflaterFactory) {
        this(IOUtil.toPath(file), compressionLevel, deflaterFactory);
    }

    /**
     * Prepare to compress at the given compression level
     * @param compressionLevel 1 <= compressionLevel <= 9
     * @param deflaterFactory custom factory to create deflaters (overrides the default)
     */
    public BlockCompressedOutputStream(final Path path, final int compressionLevel, final DeflaterFactory deflaterFactory) {
        super(path);
        deflater = deflaterFactory.makeDeflater(compressionLevel, true);
        log.debug("Using deflater: " + deflater.getClass().getSimpleName());
    }

    /**
     * Uses default compression level, which is 5 unless changed by setCompressionLevel
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * Use {@link #BlockCompressedOutputStream(OutputStream, File, int, DeflaterFactory)} to specify a custom factory.
     *
     * @param file may be null
     */
    public BlockCompressedOutputStream(final OutputStream os, final File file) {
        this(os, file, defaultCompressionLevel);
    }

    /**
     * Uses default compression level, which is 5 unless changed by setCompressionLevel
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * Use {@link #BlockCompressedOutputStream(OutputStream, File, int, DeflaterFactory)} to specify a custom factory.
     *
     * @param file may be null
     */
    public BlockCompressedOutputStream(final OutputStream os, final Path file) {
        this(os, file, defaultCompressionLevel);
    }

    /**
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * Use {@link #BlockCompressedOutputStream(OutputStream, File, int, DeflaterFactory)} to specify a custom factory.
     */
    public BlockCompressedOutputStream(final OutputStream os, final File file, final int compressionLevel) {
        this(os, file, compressionLevel, defaultDeflaterFactory);
    }

    /**
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * Use {@link #BlockCompressedOutputStream(OutputStream, File, int, DeflaterFactory)} to specify a custom factory.
     */
    public BlockCompressedOutputStream(final OutputStream os, final Path file, final int compressionLevel) {
        this(os, file, compressionLevel, defaultDeflaterFactory);
    }

    /**
     * Creates the output stream.
     * @param os output stream to create a BlockCompressedOutputStream from
     * @param file file to which to write the output or null if not available
     * @param compressionLevel the compression level (0-9)
     * @param deflaterFactory custom factory to create deflaters (overrides the default)
     */
    public BlockCompressedOutputStream(final OutputStream os, final File file, final int compressionLevel, final DeflaterFactory deflaterFactory) {
        this(os, IOUtil.toPath(file), compressionLevel, deflaterFactory);
    }

    /**
     * Creates the output stream.
     * @param os output stream to create a BlockCompressedOutputStream from
     * @param file file to which to write the output or null if not available
     * @param compressionLevel the compression level (0-9)
     * @param deflaterFactory custom factory to create deflaters (overrides the default)
     */
    public BlockCompressedOutputStream(final OutputStream os, final Path file, final int compressionLevel, final DeflaterFactory deflaterFactory) {
        super(os, file);
        deflater = deflaterFactory.makeDeflater(compressionLevel, true);
        log.debug("Using deflater: " + deflater.getClass().getSimpleName());
    }

    /**
     *
     * @param location May be null.  Used for error messages, and for checking file termination.
     * @param output May or not already be a BlockCompressedOutputStream.
     * @return A BlockCompressedOutputStream, either by wrapping the given OutputStream, or by casting if it already
     *         is a BCOS.
     */
    public static BlockCompressedOutputStream maybeBgzfWrapOutputStream(final File location, OutputStream output) {
        if (!(output instanceof BlockCompressedOutputStream)) {
           return new BlockCompressedOutputStream(output, location);
        } else {
           return (BlockCompressedOutputStream)output;
        }
    }

    /**
     * Adds a GZIIndexer to the block compressed output stream to be written to the specified output stream. See
     * {@link GZIIndex} for details on the index. Note that the stream will be written to disk entirely when close()
     * is called.
     * @throws RuntimeException this method is called after output has already been written to the stream.
     */
    public void addIndexer(final OutputStream outputStream) {
        if (mBlockAddress != 0) {
            throw new RuntimeException("Cannot add gzi indexer if this BlockCompressedOutput stream has already written Gzipped blocks");
        }
        indexer = new GZIIndex.GZIIndexer(outputStream);
    }

    /**
     * close() must be called in order to flush any remaining buffered bytes.  An unclosed file will likely be
     * defective.
     *
     */
    @Override
    public void close() throws IOException {
        close(true);
    }

    /**
     * Attempt to write the data in uncompressedBuffer to the underlying file in a gzip block.
     * If the entire uncompressedBuffer does not fit in the maximum allowed size, reduce the amount
     * of data to be compressed, and slide the excess down in uncompressedBuffer so it can be picked
     * up in the next deflate event.
     */
    @Override
    protected void deflateBlock() {
        if (numUncompressedBytes == 0) {
            return;
        }
        final int bytesToCompress = numUncompressedBytes;
        // Compress the input
        deflater.reset();
        deflater.setInput(uncompressedBuffer, 0, bytesToCompress);
        deflater.finish();
        int compressedSize = deflater.deflate(compressedBuffer, 0, compressedBuffer.length);

        // If it didn't all fit in compressedBuffer.length, set compression level to NO_COMPRESSION
        // and try again.  This should always fit.
        if (!deflater.finished()) {
            noCompressionDeflater.reset();
            noCompressionDeflater.setInput(uncompressedBuffer, 0, bytesToCompress);
            noCompressionDeflater.finish();
            compressedSize = noCompressionDeflater.deflate(compressedBuffer, 0, compressedBuffer.length);
            if (!noCompressionDeflater.finished()) {
                throw new IllegalStateException("unpossible");
            }
        }
        // Data compressed small enough, so write it out.
        crc32.reset();
        crc32.update(uncompressedBuffer, 0, bytesToCompress);

        final int totalBlockSize = writeGzipBlock(compressedBuffer, compressedSize, bytesToCompress, crc32.getValue());
        assert(bytesToCompress <= numUncompressedBytes);

        // Call out to the indexer if it exists
        if (indexer != null) {
            indexer.addGzipBlock(mBlockAddress, numUncompressedBytes);
        }

        // Clear out from uncompressedBuffer the data that was written
        numUncompressedBytes = 0;
        mBlockAddress += totalBlockSize;
    }
}
