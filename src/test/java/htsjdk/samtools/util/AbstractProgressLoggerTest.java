package htsjdk.samtools.util;

import htsjdk.HtsjdkTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AbstractProgressLoggerTest extends HtsjdkTest {

    @DataProvider
    public Object[][] getPadData() {
        return new Object[][]{
                {"hello", 10, "     hello"},
                {"hello", 6, " hello"},
                {"hello", 5, "hello"},
                {"hello", 4, "hello"},
                {"hello", -1, "hello"}
        };
    }

    @Test(dataProvider = "getPadData")
    public void testPad(String in, int len, String expected) {
        Assert.assertEquals(AbstractProgressLogger.pad(in, len), expected);
    }
}
