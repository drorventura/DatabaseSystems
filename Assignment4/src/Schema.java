import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created with IntelliJ IDEA.
 * User: Dror
 * Date: 16/12/13
 * Time: 20:46
 * To change this template use File | Settings | File Templates.
 */
public class Schema
{
    private Connection connection;

    public Schema(Connection connection)
    {
        this.connection = connection;
    }

    private String createTablePersons()
    {
        String createTablePersons = "CREATE TABLE persons(" +
                                    "id INT NOT NULL, " +
                                    "age INT, " +
                                    "workclass VARCHAR(40), " +
                                    "education VARCHAR(40), " +
                                    "education_number INT, " +
                                    "marital_status VARCHAR(40), " +
                                    "race VARCHAR(20), " +
                                    "sex VARCHAR(20), " +
                                    "capital_gain INT, " +
                                    "native_country VARCHAR(40), " +
                                    "PRIMARY KEY(id)" +
                                    ");";

        return  createTablePersons;
    }

    private String createTableMarriedAndDescendants()
    {
        String createTableMarriedAndDescendants =   "CREATE TABLE married_and_descendants(" +
                                                    "id_person INT NOT NULL, " +
                                                    "id_relative INT NOT NULL, " +
                                                    "relationship VARCHAR(20), " +
                                                    "PRIMARY KEY(id_person, id_relative), " +
                                                    "FOREIGN KEY (id_person) REFERENCES persons(id), " +
                                                    "FOREIGN KEY (id_relative) REFERENCES persons(id)" +
                                                    ");";
        return createTableMarriedAndDescendants;
    }

    private String createTableCars()
    {
        String createTableCars =    "CREATE TABLE cars(" +
                                    "car_id INT NOT NULL, " +
                                    "car_manufacturer VARCHAR(20), " +
                                    "car_model VARCHAR(20), " +
                                    "car_year INT, " +
                                    "PRIMARY KEY(car_id)" +
                                    ");";
        return createTableCars;
    }


    private String createTableCarsOwnedByPeople()
    {
        String createTableCarsOwnedByPeople =   "CREATE TABLE cars_owned_by_people(" +
                                                "person_id INT NOT NULL, " +
                                                "car_id INT NOT NULL, " +
                                                "color VARCHAR(20), " +
                                                "date_purchased DATE, " +
                                                "PRIMARY KEY(person_id, car_id), " +
                                                "FOREIGN KEY (person_id) REFERENCES persons(id), " +
                                                "FOREIGN KEY (car_id) REFERENCES cars(car_id)" +
                                                ");";
        return createTableCarsOwnedByPeople;
    }

    public void personsTriggers()
    {
        String triggerOnWorkerClass =   "CREATE TRIGGER workclass" +
                                        "BEFORE INSERT ON persons" +
                                        "FOR EACH ROW" +
                                        "BEGIN" +
                                        "IF NEW.workclass NOT IN" +
                                        "('Private', 'Self-emp-not-inc', 'Self-emp-inc', 'Federal-gov'," +
                                        "'Federal-gov', 'State-gov', 'Without-pay', 'Never-worked')" +
                                        "THEN CALL invalid_workclass_was_inserted" +
                                        "END IF;";

        String triggerOnRace =  "CREATE TRIGGER race" +
                                "BEFORE INSERT ON persons" +
                                "FOR EACH ROW" +
                                "BEGIN" +
                                "IF NEW.race NOT IN ('White', 'Asian-Pac-Islander'," +
                                "'Amer-Indian-Eskimo', 'Other', 'Black')" +
                                "THEN CALL invalid_race_was_inserted" +
                                "END IF;";

        String triggerOnEducation = "CREATE TRIGGER education" +
                                    "BEFORE INSERT ON persons" +
                                    "FOR EACH ROW" +
                                    "BEGIN" +
                                    "IF NEW.education NOT IN ('Bachelors', 'Some-college', '11th'," +
                                    "'HS-grad', 'Prof-school', 'Assoc-acdm', 'Assoc-acdm', '9th'," +
                                    "'7th-8th', '12th', 'Masters', '1st-4th', '10th', 'Doctorate'," +
                                    "'5th-6th', 'Preschool')" +
                                    "THEN CALL invalid_race_was_inserted" +
                                    "END IF;";



    }

    private void createTriggers()
    {
        Statement statement = this.connection.createStatement();



    }

    public void createSchema()
    {
        try
        {
            Statement statement = this.connection.createStatement();

            String sqlRequest = createTablePersons();
            statement.executeUpdate(sqlRequest);
            System.out.println("Table persons was created");

            statement.executeUpdate(createTableMarriedAndDescendants());
            System.out.println("Table married_and_descendants was created");

            statement.executeUpdate(createTableCars());
            System.out.println("Table cars was created");

            statement.executeUpdate(createTableCarsOwnedByPeople());
            System.out.println("Table cars_owned_by_peaple was created");

        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    public void dropSchema()
    {
        try
        {
            Statement statement = this.connection.createStatement();

            statement.execute("DROP TABLE  cars_owned_by_people");
            statement.execute("DROP TABLE married_and_descendants");
            statement.execute("DROP TABLE persons");
            statement.execute("DROP TABLE cars");
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

}
