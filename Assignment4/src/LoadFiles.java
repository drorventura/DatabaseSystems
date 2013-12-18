import java.io.FileNotFoundException;
import java.sql.*;

/**
 * Dror Ventura
 * Shay Zilberman
 */
public class LoadFiles
{
    public static void loadFileToPersons(Connection conn, String filePath)
    {
        ReadCVS cvsReader = new ReadCVS();
        String[] record;
        PreparedStatement statement = null;

        try
        {
            cvsReader.readFile(filePath);
            String insertTableRequest = "INSERT INTO persons" +
                                        "(id, age, workclass, education, education_number," +
                                        "marital_status, race, sex, capital_gain, native_country)" +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
            statement = conn.prepareStatement(insertTableRequest);
        }
        catch (SQLException | FileNotFoundException e)
        {
            e.printStackTrace();
        }

        while ((record = cvsReader.getNextRecord()) != null)
        {
            try
            {
                if (record[0].equals("?"))
                    statement.setNull(1, Types.INTEGER); // id is a primary key so that will throw an exception as expected
                else
                    statement.setInt(1, Integer.parseInt(record[0])); // id
                if(record[1].equals("?"))
                    statement.setNull(2, Types.INTEGER);
                else
                    statement.setInt(2,Integer.parseInt(record[1])); // age
                if(record[2].equals("?"))
                    statement.setNull(3, Types.VARCHAR);
                else
                    statement.setString(3, record[2]); // workclass
                if(record[3].equals("?"))
                    statement.setNull(4, Types.VARCHAR);
                else
                    statement.setString(4, record[3]); // education
                if(record[4].equals("?"))
                    statement.setNull(5, Types.INTEGER);
                else
                    statement.setInt(5, Integer.parseInt(record[4])); // education_number
                if(record[5].equals("?"))
                    statement.setNull(6, Types.VARCHAR);
                else
                    statement.setString(6,record[5]); // marital_status
                if(record[6].equals("?"))
                    statement.setNull(7, Types.VARCHAR);
                else
                    statement.setString(7,record[6]); // race
                if(record[7].equals("?"))
                    statement.setNull(8, Types.VARCHAR);
                else
                    statement.setString(8,record[7]); // sex
                if(record[8].equals("?"))
                    statement.setNull(9, Types.INTEGER);
                else
                    statement.setInt(9,Integer.parseInt(record[8])); // capital_gain
                if(record[9].equals("?"))
                    statement.setNull(10, Types.VARCHAR);
                else
                    statement.setString(10,record[9]); // native_country

                statement.executeUpdate();
            }
            catch (SQLException e)
            {
                System.out.println("person " + record[0] + " was not added because:\n" + e.getMessage());
            }
        }
    }

    public static void loadFileToCars(Connection connection, String filePath)
    {
        ReadCVS cvsReader = new ReadCVS();
        String[] record;
        PreparedStatement statement = null;

        try
        {
            cvsReader.readFile(filePath);
            String insertTableRequest = "INSERT INTO cars" +
                                        "(car_id, car_manufacturer, car_model, car_year)" +
                                        "VALUES (?, ?, ?, ?);";

            statement = connection.prepareStatement(insertTableRequest);
        }
        catch(SQLException | FileNotFoundException e)
        {
            e.printStackTrace();
        }

        while ((record = cvsReader.getNextRecord()) != null)
        {
            try
            {
                if (record[0].equals("?"))
                    statement.setNull(1, Types.INTEGER);
                else
                    statement.setInt(1,Integer.parseInt(record[0])); // car_id
                if (record[1].equals("?"))
                    statement.setNull(2, Types.VARCHAR);
                else
                    statement.setString(2, record[1]); // car_manufacturer
                if (record[2].equals("?"))
                    statement.setNull(3, Types.VARCHAR);
                    statement.setString(3, record[2]); // car_model
                if (record[3].equals("?"))
                    statement.setNull(4, Types.INTEGER);
                else
                    statement.setInt(4, Integer.parseInt(record[3])); // car_year

                statement.executeUpdate();
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
            }
        }
    }

    public static void loadFileToRelations(Connection connection, String filePath)
    {
        ReadCVS cvsReader = new ReadCVS();
        String[] record;
        PreparedStatement statement = null;

        try
        {
            cvsReader.readFile(filePath);

            String insertTableRequest = "INSERT INTO relations" +
                                        "(id_person, id_relative, relationship)" +
                                        "VALUES (?, ?, ?);";

            statement = connection.prepareStatement(insertTableRequest);
        }
        catch(SQLException | FileNotFoundException e)
        {
            e.printStackTrace();
        }

        while ((record = cvsReader.getNextRecord()) != null)
        {
            try
            {
                if (record[0].equals("?"))
                    statement.setNull(1, Types.INTEGER);
                else
                    statement.setInt(1, Integer.parseInt(record[0])); // id_person
                if (record[1].equals("?"))
                    statement.setNull(2, Types.INTEGER);
                else
                    statement.setInt(2, Integer.parseInt(record[1])); // id_relative
                if (record[2].equals("?"))
                    statement.setNull(3, Types.VARCHAR);
                else
                    statement.setString(3, record[2]); // relationship

                statement.executeUpdate();
            }
            catch (SQLException e)
            {
                System.out.println("relation was not added because:\n" + e.getMessage());
            }
        }
    }

    public static void loadFileToCarsPersons(Connection connection, String filePath)
    {
        ReadCVS cvsReader = new ReadCVS();
        String[] record;
        PreparedStatement statement = null;

        try
        {
            cvsReader.readFile(filePath);
            String insertTableRequest = "INSERT INTO cars_persons" +
                                        "(person_id, car_id, color, date_purchased)" +
                                        "VALUES (?, ?, ?, ?);";

            statement = connection.prepareStatement(insertTableRequest);
        }
        catch(SQLException | FileNotFoundException e)
        {
            e.printStackTrace();
        }

        while ((record = cvsReader.getNextRecord()) != null)
        {
            try
            {
//                if(Integer.parseInt(record[0]) > 1000)
//                    continue;

                if (record[0].equals("?"))
                    statement.setNull(1, Types.INTEGER);
                else
                    statement.setInt(1, Integer.parseInt(record[0])); // person_id
                if (record[1].equals("?"))
                    statement.setNull(2, Types.INTEGER);
                else
                    statement.setInt(2, Integer.parseInt(record[1])); // car_id
                if (record[2].equals("?"))
                    statement.setNull(3, Types.VARCHAR);
                else
                    statement.setString(3, record[2]); // color
                if (record[3].equals("?"))
                    statement.setNull(4, Types.DATE);
                else
                {
                    String[] date = record[3].split("/");
                    statement.setDate(4, Date.valueOf(date[2] + "-" + date[1] + "-" + date[0])); // date_purchased
                }

                statement.executeUpdate();
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
            }
        }

    }
}
