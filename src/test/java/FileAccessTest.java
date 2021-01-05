import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileAccessTest extends TestCase {

    final String containerAdress = "hdfs://dd1fd2116121:8020";
    FileAccess fileAccess;
    FileSystem hdfs;

    @Override
    protected void setUp() throws Exception {
        fileAccess = new FileAccess(containerAdress);
        hdfs = fileAccess.getHDFS();
    }


    public void testCreateAndList() throws IOException {
        List<String> actual;
        List<String> expected = new ArrayList<>();

        fileAccess.create(containerAdress + "/testFolder/file1.txt");
        fileAccess.create(containerAdress + "/testFolder/file2.txt");

        actual = fileAccess.list(containerAdress + "/testFolder");
        expected.add(containerAdress + "/testFolder/file1.txt");
        expected.add(containerAdress + "/testFolder/file2.txt");

        assertEquals(expected, actual);
    }

    public void testDelete() throws IOException {
        List<String> actual;
        List<String> expected = new ArrayList<>();

        fileAccess.create(containerAdress + "/testFolder/file1.txt");
        fileAccess.create(containerAdress + "/testFolder/file2.txt");

        expected.add(containerAdress + "/testFolder/file2.txt");
        fileAccess.delete(containerAdress + "/testFolder/file1.txt");
        actual = fileAccess.list(containerAdress + "/testFolder");

        assertEquals(expected, actual);
    }

    public void testAppendAndRead() throws IOException {
        String actual;
        String expected;

        fileAccess.create(containerAdress + "/testFolder/file.txt");
        fileAccess.append(containerAdress + "/testFolder/file.txt", "testWord1");
        fileAccess.append(containerAdress + "/testFolder/file.txt", " testWord2");

        actual = fileAccess.read(containerAdress + "/testFolder/file.txt");
        expected = "testWord1 testWord2";

        assertEquals(expected, actual);
    }

    public void testIsDirectory() throws IOException {
        boolean actual1;
        boolean actual2;
        boolean actual = false;

        fileAccess.create(containerAdress + "/testFolder/file.txt");
        fileAccess.create(containerAdress + "/testFolder/testFolder1/file.txt");

        actual1 = fileAccess.isDirectory(containerAdress + "/testFolder/file.txt");
        actual2 = fileAccess.isDirectory(containerAdress + "/testFolder/testFolder1");
        if (!actual1 && actual2){
            actual = true;
        }
        assertTrue(actual);
    }

    @Override
    protected void tearDown() throws Exception {
        Path path = new Path(containerAdress + "/testFolder");
        hdfs.delete(path, true);
    }
}
