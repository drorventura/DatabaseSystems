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
        return new StringBuilder()  .append("CREATE TABLE persons(")
                                    .append("id INT NOT NULL, ")
                                    .append("age INT, ")
                                    .append("workclass VARCHAR(40), ")
                                    .append("education VARCHAR(40), ")
                                    .append("education_number INT, ")
                                    .append("marital_status VARCHAR(40), ")
                                    .append("race VARCHAR(20), ")
                                    .append("sex VARCHAR(20), ")
                                    .append("capital_gain INT, ")
                                    .append("native_country VARCHAR(40), ")
                                    .append("PRIMARY KEY(id)")
                                    .append(");").toString();
    }

    private String createTableRelations()
    {
        return  "CREATE TABLE relations(" +
                "id_person INT NOT NULL, " +
                "id_relative INT NOT NULL, " +
                "relationship VARCHAR(20), " +
                "PRIMARY KEY (id_person, id_relative), " +
                "FOREIGN KEY (id_person) REFERENCES persons(id), " +
                "FOREIGN KEY (id_relative) REFERENCES persons(id)" +
                ");";
    }

    private String createTableCars()
    {
        return  "CREATE TABLE cars(" +
                "car_id INT NOT NULL, " +
                "car_manufacturer VARCHAR(20), " +
                "car_model VARCHAR(20), " +
                "car_year INT, " +
                "PRIMARY KEY(car_id)" +
                ");";
    }

    private String createTableCarsPersons()
    {
        return  "CREATE TABLE cars_persons(" +
                "person_id INT NOT NULL, " +
                "car_id INT NOT NULL, " +
                "color VARCHAR(20), " +
                "date_purchased DATE, " +
                "PRIMARY KEY(person_id, car_id), " +
                "FOREIGN KEY (person_id) REFERENCES persons(id), " +
                "FOREIGN KEY (car_id) REFERENCES cars(car_id)" +
                ");";
    }

    public String personsTrigger()
    {
        return  "CREATE TRIGGER persons_insert " +
                "BEFORE INSERT ON persons " +
                "FOR EACH ROW " +
                "BEGIN " +
                "   IF NEW.workclass NOT IN " +
                "   ('Private', 'Self-emp-not-inc', 'Self-emp-inc', 'Federal-gov'," +
                "    'Local-gov', 'State-gov', 'Without-pay', 'Never-worked') " +
                "   THEN CALL invalid_workclass_was_inserted;" +
                "   END IF;" +
                "   IF NEW.education NOT IN " +
                "   ('Bachelors', 'Some-college', '11th'," +
                "    'HS-grad', 'Prof-school', 'Assoc-acdm', 'Assoc-voc', '9th'," +
                "    '7th-8th', '12th', 'Masters', '1st-4th', '10th', 'Doctorate'," +
                "    '5th-6th', 'Preschool') " +
                "   THEN CALL invalid_education_was_inserted;" +
                "   END IF;" +
                "   IF NEW.marital_status NOT IN " +
                "   ('Married-civ-spouse', 'Divorced', 'Never-married'," +
                "    'Separated', 'Widowed', 'Married-spouse-absent', 'Married-AF-spouse') " +
                "   THEN CALL invalid_marital_status_was_inserted;" +
                "   END IF;" +
                "   IF NEW.race NOT IN ('White', 'Asian-Pac-Islander'," +
                "   'Amer-Indian-Eskimo', 'Other', 'Black') " +
                "   THEN CALL invalid_race_was_inserted;" +
                "   END IF;" +
                "   IF NEW.native_country NOT IN " +
                "   ('United-States', 'Cambodia', 'England', 'Puerto-Rico'," +
                "    'Canada', 'Germany', 'Outlying-US(Guam-USVI-etc)', 'India', 'Japan'," +
                "    'Greece', 'South', 'China', 'Cuba', 'Iran', 'Honduras', 'Philippines'," +
                "    'Ireland', 'Italy', 'Poland', 'Jamaica', 'Vietnam', 'Mexico', 'Portugal'," +
                "    'France', 'Dominican-Republic', 'Laos', 'Ecuador', 'Taiwan', 'Haiti'," +
                "    'Columbia', 'Hungary', 'Guatemala'," +
                "    'Nicaragua', 'Scotland', 'Thailand', 'Yugoslavia', 'El-Salvador'," +
                "    'Trinadad&Tobago', 'Peru', 'Hong', 'Holand-Netherlands')" +
                "   THEN CALL invalid_sex_was_inserted;" +
                "   END IF;" +
                "END;";
    }

    public String relationTriggers()
    {
        return  "CREATE TRIGGER relation_insert " +
                "BEFORE INSERT ON relations " +
                "FOR EACH ROW " +
                "BEGIN " +
                "DECLARE parent_age, child_age, man_salary, wife_salary INT; " +
                "IF NEW.relationship NOT IN " +
                "('wife', 'husband', 'child') " +
                "THEN CALL invalid_relationship_was_inserted;" +
                "END IF;" +
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
    }

    public String carPersonsTrigger()
    {
        return  "CREATE TRIGGER cars_person_insert " +
                "BEFORE INSERT ON cars_persons " +
                "FOR EACH ROW " +
                "BEGIN " +
                "IF NEW.date_purchased > CURDATE() " +
                "THEN CALL date_purchased_is_invalid;" +
                "END IF;" +
                "END;";
    }

    private void createTriggers()
    {
        try
        {
            System.out.println("creating triggers");
            Statement statement = this.connection.createStatement();

            statement.execute(personsTrigger());
            statement.execute(relationTriggers());
            statement.execute(carPersonsTrigger());
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
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

            statement.executeUpdate(createTableRelations());
            System.out.println("Table relations was created");

            statement.executeUpdate(createTableCars());
            System.out.println("Table cars was created");

            statement.executeUpdate(createTableCarsPersons());
            System.out.println("Table cars_persons was created");

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

            statement.execute("DROP TABLE  cars_persons");
            statement.execute("DROP TABLE relations");
            statement.execute("DROP TABLE persons");
            statement.execute("DROP TABLE cars");
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

}
