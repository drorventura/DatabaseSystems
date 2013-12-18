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
    // Number 1
    static final String REACHEST_PERSON = "SELECT id FROM persons AS P1 " +
                                          "WHERE capital_gain >= all(SELECT capital_gain " +
                                          "                          FROM persons AS P2 " +
                                          "                          WHERE  P1.id != P2.id);";

    // Number 2
    static final String MAX_CHILDREN =  "SELECT NOC1.id_person " +
                                        "FROM (SELECT id_person, count(*) AS children " +
                                        "      FROM relations" +
                                        "      WHERE relationship = 'child'" +
                                        "      GROUP BY id_person) AS NOC1 " +
                                        "WHERE NOC1.children >= all(SELECT NOC2.children" +
                                        "                           FROM (SELECT id_person, count(*) AS children" +
                                        "                                 FROM relations " +
                                        "                                 WHERE relationship = 'child'" +
                                        "                                 GROUP BY id_person) AS NOC2" +
                                        "                           WHERE NOC1.id_person != NOC2.id_person);";

    // Number 3
    static final String CHILD_AVG_OF_WOMEN =    "SELECT native_country, avg(num_of_children) AS average " +
                                                "FROM (SELECT native_country, mother, count(*) AS num_of_children" +
                                                "      FROM(SELECT P.id AS mother, R.id_relative AS child, P.native_country" +
                                                "           FROM relations R JOIN persons P ON R.relationship = 'child' AND R.id_person = P.id AND P.sex = 'Female') x" +
                                                "      GROUP BY native_country, mother) y" +
                                                "GROUP BY native_country;";

    // Number 4
    static final String CHILD_AVG = "SELECT avg(num_of_children) AS result " +
                                    "FROM ((SELECT person_id , count(car_id) AS num_of_cars" +
                                    "      FROM cars_persons" +
                                    "      GROUP BY person_id" +
                                    "      HAVING num_of_cars > 1) x" +
                                    "      JOIN" +
                                    "     (SELECT id_person, count(*) AS num_of_children" +
                                    "      FROM relations" +
                                    "      WHERE relationship = 'child'" +
                                    "      GROUP BY id_person) y" +
                                    "      ON x.person_id = y.id_person);";

    // Number 5
    static final String BLACK_MERCEDES_COUNTRY = "SELECT persons.native_country, count(cars_persons.color) AS black_cars " +
                                                 "FROM (persons JOIN cars_persons ON person_id = id)" +
                                                 "             JOIN cars ON cars_persons.car_id = cars.car_id " +
                                                 "WHERE cars_persons.color = 'black' AND cars.car_manufacturer = 'MERCEDES'" +
                                                 "GROUP BY native_country " +
                                                 "ORDER BY black_cars DESC;";

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
                        System.out.println("The person with id: " + reachest + " is the reachest person in data base.");
                    }
                    else
                        System.out.println("No match was found");
                    break;
                case 2:
                    resultSet = statement.executeQuery(MAX_CHILDREN);
                    if(resultSet.next())
                    {
                        int dos = resultSet.getInt("id_person");
                        System.out.println("The person with id: " + dos + " has the most children.");
                    }
                    else
                        System.out.println("No match was found");
                    break;
                case 3:
                    Main.sql(connection, CHILD_AVG_OF_WOMEN);
//                    resultSet = statement.executeQuery(CHILD_AVG_OF_WOMEN);
//                    if(resultSet.next())
//                    {
//
//                    }
//                    else
//                        System.out.println("No match was found");
                    break;
                case 4:
                    resultSet = statement.executeQuery(CHILD_AVG);
                    if(resultSet.next())
                    {
                        float avg = resultSet.getFloat("result");
                        System.out.println("The children averge is " + avg);
                    }
                    else
                        System.out.println("No match was found");
                    break;
                case 5:
                    resultSet = statement.executeQuery(BLACK_MERCEDES_COUNTRY);
                    if(resultSet.next())
                    {
                        String ars_country = resultSet.getString("native_country");
                        System.out.println("The country with the most black mercedeses is " + ars_country);
                    }
                    else
                        System.out.println("No match was found");
                    break;
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }
}
