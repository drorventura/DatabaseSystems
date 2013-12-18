import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Dror Ventura
 * Shay Zilberman
 */
public class Main
{
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:1234/dbs141_user5";
    // mysqlsrv1.cs.bgu.ac.il:3306/dbs141_user5

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
    public static String getReport(int num)
    {
        String report = null;
        switch (num)
        {
            case 1:
                report =    "SELECT id_person, id_relative, relationship, age " +
                            "FROM relations INNER JOIN persons ON id_relative = id " +
                            "WHERE Relationship = 'child' " +
                            "ORDER BY id_person ASC, age DESC;";
                break;
            case 2:
                report =    "SELECT person_id, car_id, date_purchased " +
                            "FROM cars_persons " +
                            "ORDER BY person_id ASC, date_purchased ASC";
                break;
        }
        return report;
    }

    public static void sql(Connection connection, String sqlQuery)
    {
        ArrayList<String> columnNames = new ArrayList<>();
        try
        {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlQuery);
            if (resultSet != null)
            {
                ResultSetMetaData columns = resultSet.getMetaData();
                int i = 0;
                while (i < columns.getColumnCount())
                {
                    i++;
                    System.out.printf("%-25s", columns.getColumnName(i));
                    columnNames.add(columns.getColumnName(i));
                }
                System.out.println();

                while (resultSet.next())
                {
                    for (i = 0; i < columnNames.size(); i++)
                    {
                        System.out.printf("%-25s",resultSet.getString(columnNames.get(i)));
                    }
                    System.out.println();
                }
            }
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
            e.printStackTrace();
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
                        String sqlQuery = "";
                        for(int i = 1 ; i < parsedCommad.length ; i++)
                        {
                            sqlQuery = sqlQuery + parsedCommad[i] + " ";
                        }
                        sql(connection, sqlQuery.toLowerCase());
                        break;

                    case "load":
                        String filePath = parsedCommad[1];
                        String tableNameInput = parsedCommad[2];
                        load(connection, filePath, tableNameInput);
                        break;

                    case "report":

                        sql(connection, getReport(Integer.parseInt(parsedCommad[1])));
                        break;

                    case "query":
                        Query.query(connection, Integer.parseInt(parsedCommad[1]));
                        break;

                    case "quit":
                        scanner.close();
                        System.out.println("Goodbye!");
                        break done;
                    default:
                        throw new IOException("Invalid command");
                }
            }
            catch (IndexOutOfBoundsException e)
            {
                System.out.println("Not enough parameters!");
            }
            catch (SQLException | IOException e)
            {
                System.out.println(e.getMessage());
            }
        }
    }
}
