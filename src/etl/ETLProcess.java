package src.etl;

import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class ETLProcess {

    public static void main(String[] args) {
        Connection sourceConnection = connectToDatabase("db.origen.url", "db.origen.user", "db.origen.password");
        Connection destinationConnection = connectToDatabase("db.destino.url", "db.destino.user", "db.destino.password");

        if (sourceConnection != null && destinationConnection != null) {
            System.out.println("Conexión a las bases de datos establecida con éxito.");
            extractTransformLoad(sourceConnection, destinationConnection);
        } else {
            System.out.println("Error al conectar a las bases de datos.");
        }
    }

    private static Connection connectToDatabase(String urlKey, String userKey, String passwordKey) {
        try {
            Properties properties = new Properties();
            InputStream input = ETLProcess.class.getClassLoader().getResourceAsStream("config.properties");

            if (input == null) {
                System.out.println("El archivo config.properties no se encuentra en el classpath.");
                return null;
            }

            properties.load(input);
            input.close();

            String url = properties.getProperty(urlKey);
            String user = properties.getProperty(userKey);
            String password = properties.getProperty(passwordKey);

            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            return DriverManager.getConnection(url, user, password);

        } catch (Exception e) {
            System.out.println("Error al conectar a la base de datos:");
            e.printStackTrace();
            return null;
        }
    }

    private static void extractTransformLoad(Connection sourceConnection, Connection destinationConnection) {
        String query = "SELECT id, nombre FROM dbo.Clientes"; 
        String insertQuery = "INSERT INTO dbo.ClientesDestino (id, nombre) VALUES (?, ?)";

        try (
            Statement stmt = sourceConnection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            PreparedStatement pstmt = destinationConnection.prepareStatement(insertQuery)
        ) {
            while (rs.next()) {
                int id = rs.getInt("id");
                String nombre = rs.getString("nombre").toUpperCase(); // Transformación a mayúsculas

                pstmt.setInt(1, id);
                pstmt.setString(2, nombre);
                pstmt.executeUpdate();
                System.out.println("Insertado: " + id + " - " + nombre);
            }

        } catch (SQLException e) {
            System.out.println("Error en el proceso ETL:");
            e.printStackTrace();
        }
    }
}
