import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Dror Ventura
 * Shay Zilberman
 */
public class Query
{
    static final String REACHEST_PERSON = "SELECT id FROM persons AS P1 " +
                                          "WHERE capital_gain > all(SELECT capital_gain " +
                                          "                         FROM persons as P2 " +
                                          "                         WHERE  P1.id != P2.id);";

    static final String MAX_CHILDREN =  "SELECT NOC1.id_person " +
                                        "FROM (SELECT id_person, count(*) AS children FROM relations WHERE relationship = 'child' GROUP BY id_person) AS NOC1 " +
                                        "WHERE NOC1.children > all(SELECT NOC2.children" +
                                        "                          FROM (SELECT id_person, count(*) AS children FROM relations WHERE relationship = 'child' GROUP BY id_person) AS NOC2" +
                                        "                          WHERE NOC1.id_person != NOC2.id_person);";


    static final String CHILD_AVG = "WITH more_then_one_car(id_person) AS" +
                                    "(SELECT id_person FROM cars_owned_by_people " +
                                    "HAVING count(car_id) > 1)," +
                                    "number_of_children(id_person,num_of_children) AS" +
                                    "(SELECT id_person, count(*)" +
                                    "FROM married_and_descendants " +
                                    "WHERE relationship = 'child'" +
                                    "GROUP BY id_person)"+
                                    "SELECT avg(num_of_children)"+
                                    "FROM more_then_one_car NATURAL JOIN number_of_children;";

    static final String BLACK_MERCEDES_COUNTRY = "";

    public static void query(Connection connection, int i)
    {
        try
        {
            Statement statement = connection.createStatement();
            ResultSet resultSet;
            switch (i)
            {
                case 1:
                    resultSet = statement.executeQuery(REACHEST_PERSON);
                    if(resultSet.next())
                    {
                        int reachest = resultSet.getInt("id");
                        System.out.println("The person with id:" + reachest + " is the reachest person in data base.");
                    }
                    else
                        System.out.println("No match was found");
                    break;
                case 2:
                    resultSet = statement.executeQuery(MAX_CHILDREN);
                    if(resultSet.next())
                    {
                        int dos = resultSet.getInt("id_person");
                        System.out.println(dos + " has the most children.");
                    }
                    else
                        System.out.println("No match was found");
                    break;
                case 3:
                    resultSet = statement.executeQuery(CHILD_AVG);
                    int avg = resultSet.getInt("kids_avg");
                    System.out.println("The children averge is " + avg);
                    break;
                case 4:
                    resultSet = statement.executeQuery(BLACK_MERCEDES_COUNTRY);
                    int ars_country = resultSet.getInt("native_country");
                    System.out.println("The country with the most black mercedeses is " + ars_country);
                    break;
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }
}
