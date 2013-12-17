import java.io.IOException;
import java.sql.*;
import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * User: Dror
 * Date: 16/12/13
 * Time: 14:59
 * To change this template use File | Settings | File Templates.
 */
public class Main
{
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:1234/dbs141_user5";

    //  Database credentials
    static final String USER = "dbs141_user5";
    static final String PASS = "dbsdbs";

    public static Connection connectToDatabase()
    {
        Connection conn = null;
        try
        {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection( DB_URL,
                                                USER,
                                                PASS);
        }
        catch (SQLException | ClassNotFoundException e)
        {
            e.printStackTrace();
        }
        return conn;
    }

    public static void load(Connection connection, String filePath, String tableNameInput) throws IOException
    {
        switch (tableNameInput)
        {
            case "persons":
                System.out.println("Loading file Persons to table");
                LoadFiles.loadFileToPersons(connection, filePath);
                System.out.println("Records has been added");
                break;
            case "relations":
                System.out.println("Loading file MarriedAndDescendants to table");
                LoadFiles.loadFileToRelations(connection, filePath);
                System.out.println("Records has been added");
                break;
            case "cars":
                System.out.println("Loading file Cars to table");
                LoadFiles.loadFileToCars(connection, filePath);
                System.out.println("Records has been added");
                break;
            case "cars_persons":
                System.out.println("Loading file CarsOwnedByPeople to table");
                LoadFiles.loadFileToCarsPersons(connection, filePath);
                System.out.println("Records has been added");
                break;
            default:
                throw new IOException("No such table " + tableNameInput);
        }
    }

    public static void main(String[] args)
    {
        System.out.println("Connecting to server");
        Connection connection = connectToDatabase();
        System.out.println("Connected");

        Schema schema = new Schema(connection);
        PrintTable printTable = new PrintTable(connection);

        done:
        while (true)
        {
            System.out.print(">>");
            Scanner scanner = new Scanner(System.in).useDelimiter(";");
            String[] parsedCommad = scanner.next().split("\\s");

            String action = "";
            if (parsedCommad.length > 0)
                action = parsedCommad[0];

            try
            {
                switch (action)
                {
                    case "create":
                        System.out.println("Creating Schema");
                        schema.createSchema();
                        System.out.println("Creating was created successfully");
                        break;

                    case "drop":
                        schema.dropSchema();
                        break;

                    case "print":
                        printTable.printSchema(parsedCommad[1]);
                        break;

                    case "sql":
    //                    sql(s);
                        break;

                    case "load":
                        String filePath = parsedCommad[1];
                        String tableNameInput = parsedCommad[2];
                        load(connection, filePath, tableNameInput);
                        break;

                    case "report":
//                        report(s.nextInt());
                        break;

                    case "query":
//                        query(s.nextInt());
                        break;

                    case "quit":
                        scanner.close();
                        System.out.println("Goodbye!");
                        break done;
                    default:
                        throw new IOException("Invalid command");
                }
            }
            catch (IOException e)
            {
                System.out.println(e.getMessage());
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
            }
        }
    }
}
