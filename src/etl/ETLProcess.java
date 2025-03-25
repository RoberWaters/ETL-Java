package src.etl;

import java.io.InputStream;
import java.sql.*;
import java.util.Properties;
import java.util.Scanner;

public class ETLProcess {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        try {
            // El usuario ingresa la consulta SQL completa
            System.out.print("Ingrese la consulta SQL para extraer los datos: ");
            String query = scanner.nextLine();

            System.out.print("Ingrese el nombre de la tabla de destino: ");
            String tableDestination = scanner.nextLine();

            Connection sourceConnection = connectToDatabase("db.origen.url", "db.origen.user", "db.origen.password");
            Connection destinationConnection = connectToDatabase("db.destino.url", "db.destino.user", "db.destino.password");

            if (sourceConnection != null && destinationConnection != null) {
                System.out.println("Conexión a las bases de datos establecida con éxito.");
                extractTransformLoad(sourceConnection, destinationConnection, query, tableDestination);
            } else {
                System.out.println("Error al conectar a las bases de datos.");
            }

            // Cerrar conexiones
            if (sourceConnection != null) sourceConnection.close();
            if (destinationConnection != null) destinationConnection.close();

        } catch (Exception e) {
            System.out.println("Error en la ejecución del ETL:");
            e.printStackTrace();
        } finally {
            scanner.close();
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

    private static void extractTransformLoad(Connection sourceConnection, Connection destinationConnection, String query, String tableDestination) {
        try (Statement stmt = sourceConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            if (columnCount < 1) {
                System.out.println("La consulta no devolvió datos.");
                return;
            }

            // Construir consulta INSERT dinámica
            StringBuilder insertQuery = new StringBuilder("INSERT INTO ").append(tableDestination).append(" VALUES (");
            for (int i = 0; i < columnCount; i++) {
                insertQuery.append("?");
                if (i < columnCount - 1) insertQuery.append(", ");
            }
            insertQuery.append(")");

            try (PreparedStatement pstmt = destinationConnection.prepareStatement(insertQuery.toString())) {
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        Object value = rs.getObject(i);

                        // Aplicar transformaciones a cadenas de texto
                        if (value instanceof String) {
                            value = ((String) value).toUpperCase(); // Convierte a mayúsculas
                        }

                        pstmt.setObject(i, value);
                    }
                    pstmt.executeUpdate();
                }
                System.out.println("ETL finalizado correctamente.");
            }

        } catch (SQLException e) {
            System.out.println("Error en el proceso ETL:");
            e.printStackTrace();
        }
    }
}
