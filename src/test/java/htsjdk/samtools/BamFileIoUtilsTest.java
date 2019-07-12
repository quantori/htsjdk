package htsjdk.samtools;

import htsjdk.HtsjdkTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;

public class BamFileIoUtilsTest extends HtsjdkTest {

    @DataProvider
    public Object[][] getFilenames() {
        return new Object[][]{
                {"test.bam", true},
                {"test.sam.bam", true},
                {".bam", true},
                {"./test.bam", true},
                {"./test..bam", true},
                {"c:\\path\\to\test.bam", true},
                {"test.Bam", false},
                {"test.BAM", false},
                {"test.sam", false},
                {"test.bam.sam", false},
                {"testbam", false}
        };
    }

    @Test(dataProvider = "getFilenames")
    public void testIsBamFile(String file, boolean isBam) {
        Assert.assertEquals(BamFileIoUtils.isBamFile(new File(file)), isBam);
    }
}