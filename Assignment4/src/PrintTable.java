import java.sql.*;
import java.util.Date;

/**
 * Dror Ventura
 * Shay Zilberman
 */
public class PrintTable
{
    private Connection connection;
    private String queryForPersons;
    private String queryForMarriedAndDesendets;
    private String queryForCars;
    private String queryForCarsOwned;

    public PrintTable(Connection connection)
    {
        this.connection = connection;
        this.queryForPersons = "SELECT * FROM persons";
        this.queryForMarriedAndDesendets = "SELECT * FROM relations";
        this.queryForCars = "SELECT * FROM cars";
        this.queryForCarsOwned = "SELECT * from cars_persons";
    }

    public void printSchema(String table) throws SQLException
    {
        ResultSet resultSet;
        Statement statement = this.connection.createStatement();
        switch (table)
        {
            case "persons":
                resultSet = statement.executeQuery(this.queryForPersons);
                printPersons(resultSet);
                break;
            case "relations":
                resultSet = statement.executeQuery(this.queryForMarriedAndDesendets);
                printMarried(resultSet);
                break;
            case "cars":
                resultSet = statement.executeQuery(this.queryForCars);
                printCars(resultSet);
                break;
            case "cars_persons":
                resultSet = statement.executeQuery(this.queryForCarsOwned);
                printCarsOwned(resultSet);
                break;
            default:
                throw new SQLException("No such table in database");
        }
    }

    public void printPersons(ResultSet resultSet) throws SQLException
    {
        System.out.printf("%-5s %-5s %-20s %-10s %-15s %-25s %-25s %-7s %-15s %-15s",
                          "ID", "Age", "WorkClass", "Education", "Education-Num", "Marital-status",
                          "Race", "Sex", "Capital-gain", "Native-country");
        System.out.println();

        while(resultSet.next())
        {
            //Retrieve by column name
            int id  = resultSet.getInt("id");
            int age = resultSet.getInt("age");
            String workClass = resultSet.getString("workclass");
            String education = resultSet.getString("education");
            int educationNum = resultSet.getInt("education_number");
            String marry = resultSet.getString("marital_status");
            String race = resultSet.getString("race");
            String sex = resultSet.getString("sex");
            int capitalGain = resultSet.getInt("capital_gain");
            String nativeCountry = resultSet.getString("native_country");

            //Display values
            System.out.printf("%-5s %-4s %-20s %-15s %-10s %-25s %-25s %-10s %-12s %s",
                              id, age,workClass, education, educationNum, marry,
                              race, sex, capitalGain, nativeCountry);
            System.out.println();
        }
    }

    public void printMarried(ResultSet resultSet) throws SQLException
    {
        System.out.printf("%-10s %-10s %s", "Id_person", "Id_relative", "Relationship");
        System.out.println();

        while(resultSet.next())
        {
            //Retrieve by column name
            int id_person  = resultSet.getInt("id_person");
            int id_relative = resultSet.getInt("id_relative");
            String relationship = resultSet.getString("Relationship");

            //Display values
            System.out.printf("%-12s" , id_person);
            System.out.printf("%-12s" , id_relative);
            System.out.printf("%s" , relationship);
            System.out.println();
        }
    }

    public void printCars(ResultSet resultSet) throws SQLException
    {
            System.out.printf("%-10s %-20s %-15s %s", "Car_Id", "Car_manufacturer", "Car_model", "Car_Year");
        System.out.println();

        while(resultSet.next())
        {
            //Retrieve by column name
            int carID  = resultSet.getInt("car_id");
            String carManufacturer = resultSet.getString("car_manufacturer");
            String carModel = resultSet.getString("car_model");
            int carYear = resultSet.getInt("car_year");


            //Display values
            System.out.printf("%-15s" , carID);
            System.out.printf("%-18s" , carManufacturer);
            System.out.printf("%-15s" , carModel);
            System.out.printf("%s" , carYear);
            System.out.println();
        }
    }

    public void printCarsOwned(ResultSet resultSet) throws SQLException
    {
        System.out.printf("%-13s %-10s %-9s %s", "Person_Id", "Car_Id", "Color", "Date_purchased");
        System.out.println();

        while(resultSet.next())
        {
            //Retrieve by column name
            int personID  = resultSet.getInt("person_id");
            int carID = resultSet.getInt("car_id");
            String color = resultSet.getString("color");
            Date datePurchased = resultSet.getDate("date_purchased");

            //Display values
            System.out.printf("%-15s" , personID);
            System.out.printf("%-10s" , carID);
            System.out.printf("%-10s" , color);
            System.out.printf("%s" , datePurchased.toString());
            System.out.println("");
        }
    }
}
