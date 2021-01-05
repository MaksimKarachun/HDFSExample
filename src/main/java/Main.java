import java.util.List;

public class Main
{
    private static final String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String containerAdress = "hdfs://dd1fd2116121:8020";

    public static void main(String[] args) throws Exception
    {

        FileAccess fileAccess = new FileAccess(containerAdress);

        fileAccess.delete(containerAdress + "/test");

        //create file and directory
        fileAccess.create(containerAdress + "/test/file1.txt");

        //append file
        for (int i = 0; i < 100; i++){
            fileAccess.append(containerAdress + "/test/file1.txt", getRandomWord());
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
