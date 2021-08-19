package tests;

import databaseConnect.JDBCConnection;
import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Checking the connection to the sakila database and sending requests")

public class TestDatabase extends TestsSetup {
    @Test
    @Order(1)
    @DisplayName("Checking the creation of the students table in the sakila DB")
    public void testCreateTableStudents() {
        String query = "CREATE TABLE students ("
                + "ID int(6) NOT NULL,"
                + "FIRST_NAME VARCHAR(45) NOT NULL,"
                + "LAST_NAME VARCHAR(45) NOT NULL,"
                + "TOWN VARCHAR(45),"
                + "PRIMARY KEY (id))";
        JDBCConnection.createTable(query);
    }

    @Test
    @Order(2)
    @DisplayName("insert data in table")
    public void testInsertRequest() {
        String query = "INSERT INTO students (ID, FIRST_NAME, LAST_NAME, TOWN) VALUES (1, 'David', 'Brown', 'New York')";
        JDBCConnection.insertIntoTable(query);
        String selectQuery = "SELECT * FROM students";
        ResultSet rs = JDBCConnection.selectFromTable(selectQuery);
        assertAll("Should return inserted data",
                () -> assertEquals("1", rs.getString("ID")),
                () -> assertEquals("David", rs.getString("FIRST_NAME")),
                () -> assertEquals("Brown", rs.getString("LAST_NAME")),
                () -> assertEquals("New York", rs.getString("TOWN")));
    }

    @Test
    @Order(3)
    @DisplayName("Update data in a table")
    public void testUpdateRequest() throws SQLException {
        String query = "UPDATE students SET LAST_NAME  = 'HORST' WHERE ID=1";
        JDBCConnection.updateInTable(query);
        String selectQuery = "SELECT LAST_NAME FROM students WHERE ID=1";
        ResultSet rs = JDBCConnection.selectFromTable(selectQuery);
        String expectedTown = "HORST";
        String actualTown = rs.getString("LAST_NAME");
        assertEquals(expectedTown, actualTown, "Actual town is '" + actualTown + "'. Expected - '" + expectedTown + "'.");
    }

    @Test
    @Order(4)
    @DisplayName("Sending a simple SELECT query. Checking the address")
    public void testSelectRequest_checkAddress() throws SQLException {
        String query = "SELECT * FROM address WHERE address_id=1";
        ResultSet rs = JDBCConnection.selectFromTable(query);
        String expectedAddress = "47 MySakila Drive";
        String actualAddress = rs.getString("address");
        assertEquals(expectedAddress, actualAddress, "Actual address is '" + actualAddress + "'. Expected - '" + expectedAddress + "'.");
    }

    @Test
    @Order(5)
    @DisplayName("Отправка SELECT JOIN запроса. Проверка принадлежности города стране")
    public void testSelectWithJoinRequest_CheckCityAndCountry() throws SQLException {
        String query = "SELECT ct.city, cntr.country FROM city ct LEFT JOIN country cntr ON ct.country_id=cntr.country_id WHERE city='Bratislava'";
        ResultSet rs = JDBCConnection.selectFromTable(query);
        String expectedCountry = "Slovakia";
        String actualCountry = rs.getString("country");
        assertEquals(expectedCountry, actualCountry, "Actual country is '" + actualCountry + "'. Expected - '" + expectedCountry + "'.");
    }

    @Test
    @Order(6)
    @DisplayName("Отправка SELECT JOIN запроса. Проверка языка последнего в списке фильма")
    public void testSelectWithJoinRequest_CheckFilmLanguage() throws SQLException {
        String query = "SELECT f.title, l.name FROM film f LEFT JOIN language l ON f.language_id=l.language_id";
        ResultSet rs = JDBCConnection.selectFromTable(query);
        rs.last();
        String expectedLanguage = "English";
        String actualLanguage = rs.getString("name");
        assertEquals(expectedLanguage, actualLanguage, "Actual language is '" + actualLanguage + "'. Expected - '" + expectedLanguage + "'.");
    }

    @Test
    @Order(7)
    @DisplayName("Deleting data from the table")
    public void testDeleteRequest() {
        String query = "DELETE FROM students WHERE ID=1";
        JDBCConnection.deleteFromTable(query);
    }

    @Test
    @Order(8)
    @DisplayName("Deleting the Table from the Database")
    public void test_dropTable() {
        JDBCConnection.dropTable("students");
    }


}
