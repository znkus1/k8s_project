package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresConnect implements DBConnect {

    @Override
    public Connection getConnection() {
        String url = "jdbc:postgresql://172.31.3.234:5432/ml";
        String user = "postgres";
        String password = "postgres";

        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
