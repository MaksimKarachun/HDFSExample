import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class FileAccess
{
    /**
     * Initializes the class, using rootPath as "/" directory
     *
     * @param rootPath - the path to the root of HDFS,
     * for example, hdfs://localhost:32771
     */

    private Configuration configuration;
    private FileSystem hdfs;

    public FileAccess(String rootPath) throws URISyntaxException, IOException {
        configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.setBoolean("dfs.support.append", true);
        configuration.set("dfs.replication", "1");
        System.setProperty("HADOOP_USER_NAME", "root");
        hdfs = FileSystem.get(new URI(rootPath), configuration);
    }

    public FileSystem getHDFS(){
        return hdfs;
    }

    /**
     * Creates empty file or directory
     *
     * @param path
     */
    public void create(String path) throws IOException {

        Path pathFile = new Path(path);
        if (hdfs.exists(pathFile)) {
            System.out.printf("File or directory %s already exists!\n", pathFile);
        } else {
            if (isDirectory(path)) {
                hdfs.mkdirs(pathFile);
                System.out.println("Directory create");
            } else {
                hdfs.createNewFile(pathFile);
                System.out.println("File create");
            }
        }
    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content) throws IOException {
        Path pathFile = new Path(path);
        if (hdfs.exists(pathFile) && !isDirectory(path)) {
            try(FSDataOutputStream fs = hdfs.append(pathFile);
                PrintWriter writer = new PrintWriter(fs)) {
                writer.append(content);
                writer.flush();
                fs.hflush();
                System.out.printf("File %s append\n", path);
            }
        } else {
            System.out.println("File not exist");
        }
    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path) throws IOException {
        Path filePath = new Path(path);
        FSDataInputStream inputStream = hdfs.open(filePath);
        //Classical input stream usage
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        String line;
        StringBuilder stringBuilder = new StringBuilder();
        while ((line = bufferedReader.readLine())!=null){
            stringBuilder.append(line);
        }
        inputStream.close();
        return stringBuilder.toString();
    }

    /**
     * Deletes file or directory
     *
     * @param path
     */
    public void delete(String path) throws IOException {
        Path pathFile = new Path(path);
        hdfs.delete(pathFile, true);
    }

    /**
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public boolean isDirectory(String path) throws IOException {
        Path pathFile = new Path(path);
        return hdfs.isDirectory(pathFile);
    }

    /**
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public List<String> list(String path) throws IOException {
        List<String> fileStructureList = new ArrayList<>();
        if (isDirectory(path)){
            Path pathFile = new Path(path);
            FileStatus[] files = hdfs.listStatus(pathFile);
            for (FileStatus file : files){
                fileStructureList.add(file.getPath().toString());
            }
        }
        return fileStructureList;
    }

}
