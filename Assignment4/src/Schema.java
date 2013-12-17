import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

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

    public String personsTrigger()
    {

        String personsInsertTrigger =   "CREATE TRIGGER persons_insert " +
                                        "BEFORE INSERT ON persons " +
                                        "FOR EACH ROW " +
                                        "BEGIN " +
                                        "IF NEW.workclass NOT IN " +
                                        "('Private', 'Self-emp-not-inc', 'Self-emp-inc', 'Federal-gov'," +
                                        "'Local-gov', 'State-gov', 'Without-pay', 'Never-worked') " +
                                        "THEN CALL invalid_workclass_was_inserted;" +
                                        "END IF;" +
                                        "IF NEW.education NOT IN " +
                                        "('Bachelors', 'Some-college', '11th'," +
                                        "'HS-grad', 'Prof-school', 'Assoc-acdm', 'Assoc-voc', '9th'," +
                                        "'7th-8th', '12th', 'Masters', '1st-4th', '10th', 'Doctorate'," +
                                        "'5th-6th', 'Preschool') " +
                                        "THEN CALL invalid_education_was_inserted;" +
                                        "END IF;" +
                                        "IF NEW.marital_status NOT IN " +
                                        "('Married-civ-spouse', 'Divorced', 'Never-married'," +
                                        "'Separated', 'Widowed', 'Married-spouse-absent', 'Married-AF-spouse') " +
                                        "THEN CALL invalid_marital_status_was_inserted;" +
                                        "END IF;" +
                                        "IF NEW.race NOT IN ('White', 'Asian-Pac-Islander'," +
                                        "'Amer-Indian-Eskimo', 'Other', 'Black') " +
                                        "THEN CALL invalid_race_was_inserted;" +
                                        "END IF;" +
                                        "IF NEW.native_country NOT IN " +
                                        "('United-States', 'Cambodia', 'England', 'Puerto-Rico'," +
                                        "'Canada', 'Germany', 'Outlying-US(Guam-USVI-etc)', 'India', 'Japan'," +
                                        "'Greece', 'South', 'China', 'Cuba', 'Iran', 'Honduras', 'Philippines'," +
                                        "'Ireland', 'Italy', 'Poland', 'Jamaica', 'Vietnam', 'Mexico', 'Portugal'," +
                                        "'France', 'Dominican-Republic', 'Laos', 'Ecuador', 'Taiwan', 'Haiti'," +
                                        "'Columbia', 'Hungary', 'Guatemala'," +
                                        "'Nicaragua', 'Scotland', 'Thailand', 'Yugoslavia', 'El-Salvador'," +
                                        "'Trinadad&Tobago', 'Peru', 'Hong', 'Holand-Netherlands')" +
                                        "THEN CALL invalid_sex_was_inserted;" +
                                        "END IF;" +
                                        "END;";

        return personsInsertTrigger;
    }

    public String marriedAndDescendantsTrigger()
    {

        String marriedAndDescendantsInsertTrigger = "CREATE TRIGGER persons_insert " +
                                                    "BEFORE INSERT ON persons " +
                                                    "FOR EACH ROW " +
                                                    "BEGIN " +
                                                    "IF NEW.relationship NOT IN " +
                                                    "('wife', 'husband', 'child') " +
                                                    "THEN CALL invalid_relationship_was_inserted;" +
                                                    "END IF;" +
                                                    "DECLARE parent_age, child_age, man_salary, wife_salary INT" +
                                                    "IF NEW.relationship = 'child' THEN " +
                                                    "   SET parent_age = (SELECT age FROM persons" +
                                                    "                     WHERE id = NEW.id_person);" +
                                                    "   SET child_age = (SELECT age FROM persons" +
                                                    "                    WHERE id = NEW.id_relative);" +
                                                    "   IF parent_age <= child_age " +
                                                    "   THEN CALL child_is_older_then_parent;" +
                                                    "   END IF;" +
                                                    "END IF;" +
                                                    "IF NEW.relationship = 'wife' THEN" +
                                                    "   SET man_salary = (SELECT capital_gain" +
                                                    "                     FROM persons" +
                                                    "                     WHERE id = NEW.id_person);" +
                                                    "   SET wife_salary = (SELECT capital_gain" +
                                                    "                      FROM persons" +
                                                    "                      WHERE id = NEW.id_relative);" +
                                                    "   IF man_salary < wife_salary " +
                                                    "   THEN CALL a_man_cannot_earn_more_than_his_wife;" +
                                                    "   END IF;" +
                                                    "END IF;" +
                                                    "IF NEW.relationship = 'husband' THEN" +
                                                    "   SET wife_salary = (SELECT capital_gain" +
                                                    "                     FROM persons" +
                                                    "                     WHERE id = NEW.id_person);" +
                                                    "   SET man_salary = (SELECT capital_gain" +
                                                    "                      FROM persons" +
                                                    "                      WHERE id = NEW.id_relative);" +
                                                    "   IF man_salary < wife_salary " +
                                                    "   THEN CALL a_man_cannot_earn_more_than_his_wife;" +
                                                    "   END IF;" +
                                                    "END IF;" +
                                                    "END;";
        return marriedAndDescendantsInsertTrigger;
    }
    private void createTriggers()
    {
        try
        {
            System.out.println("creating triggers");
            Statement statement = this.connection.createStatement();

            statement.execute(personsTrigger());
            statement.execute(marriedAndDescendantsTrigger());

        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
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

            createTriggers();

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
//            statement.execute("DROP Trigger workclass");
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

}
