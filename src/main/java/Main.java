import java.util.List;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String[] args) throws Exception
    {

        FileAccess fileAccess = new FileAccess("hdfs://f72cf1a87f64:8020");

        //fileAccess.delete("hdfs://f72cf1a87f64:8020/test");

        //create file and directory
        fileAccess.create("hdfs://f72cf1a87f64:8020/test/file1.txt");
        fileAccess.create("hdfs://f72cf1a87f64:8020/test/file2.txt");
        fileAccess.create("hdfs://f72cf1a87f64:8020/test/test2/file1.txt");
        fileAccess.create("hdfs://f72cf1a87f64:8020/test/test2/file2.txt");

        for (int i = 0; i < 5; i++) {
            fileAccess.append("hdfs://f72cf1a87f64:8020/test/file1.txt", getRandomWord() + " ");
            fileAccess.append("hdfs://f72cf1a87f64:8020/test/file2.txt", getRandomWord() + " ");
        }

        //append string to file
        fileAccess.append("hdfs://f72cf1a87f64:8020/test/file1.txt", "TEST words ");
        fileAccess.append("hdfs://f72cf1a87f64:8020/test/file2.txt", "TEST words ");

        //Read file and print
        System.out.println("\nThe content of the file:");
        System.out.println(fileAccess.read("hdfs://f72cf1a87f64:8020/test/file1.txt") + '\n');

        //print directory structure
        List<String> list = fileAccess.list("hdfs://f72cf1a87f64:8020/test");
        for (String file : list){
            System.out.println(file);
        }
    }

    private static String getRandomWord()
    {
        StringBuilder builder = new StringBuilder();
        int length = 2 + (int) Math.round(10 * Math.random());
        int symbolsCount = symbols.length();
        for(int i = 0; i < length; i++) {
            builder.append(symbols.charAt((int) (symbolsCount * Math.random())));
        }
        return builder.toString();
    }
}
