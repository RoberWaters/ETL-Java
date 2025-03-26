package src.etl;

import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class ETLProcess {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        try {
            Connection sourceConnection = connectToDatabase("db.origen.url", "db.origen.user", "db.origen.password");
            Connection destinationConnection = connectToDatabase("db.destino.url", "db.destino.user", "db.destino.password");

            if (sourceConnection != null && destinationConnection != null) {
                System.out.println("Conexión a las bases de datos establecida con éxito.");
                System.out.println("Seleccione el método de extracción:");
                System.out.println("1. Ingresar consulta SQL manualmente");
                System.out.println("2. Seleccionar una tabla de la base de datos");
                int opcion = scanner.nextInt();
                scanner.nextLine();

                String query;
                if (opcion == 1) {
                    System.out.print("Ingrese la consulta SQL para extraer los datos: ");
                    query = scanner.nextLine();
                } else {
                    query = seleccionarTabla(scanner, sourceConnection);
                }

                System.out.print("Ingrese el nombre de la tabla de destino: ");
                String tableDestination = scanner.nextLine();
                extractTransformLoad(sourceConnection, destinationConnection, query, tableDestination);
            } else {
                System.out.println("Error al conectar a las bases de datos.");
            }

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

    private static String seleccionarTabla(Scanner scanner, Connection connection) throws SQLException {
        System.out.println("Obteniendo lista de tablas disponibles...");
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});

        List<String> tablaNombres = new ArrayList<>();
        while (tables.next()) {
            String tableName = tables.getString("TABLE_NAME");
            System.out.println("- " + tableName);
            tablaNombres.add(tableName);
        }

        System.out.print("Ingrese el nombre de la tabla origen: ");
        String tablaOrigen = scanner.nextLine();

        System.out.println("Columnas disponibles en la tabla de origen:");
        ResultSet columns = metaData.getColumns(null, null, tablaOrigen, "%");
        List<String> columnasDisponibles = new ArrayList<>();
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            System.out.println("- " + columnName);
            columnasDisponibles.add(columnName);
        }

        System.out.print("Ingrese los nombres de las columnas a exportar, separados por comas: ");
        String[] columnasSeleccionadas = scanner.nextLine().split(",");
        List<String> columnasValidas = Arrays.stream(columnasSeleccionadas)
                .map(String::trim)
                .filter(columnasDisponibles::contains)
                .collect(Collectors.toList());

        if (columnasValidas.isEmpty()) {
            throw new IllegalArgumentException("No se seleccionaron columnas válidas.");
        }

        return "SELECT " + String.join(", ", columnasValidas) + " FROM " + tablaOrigen;
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

            List<String> columnas = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                columnas.add(metaData.getColumnName(i));
            }

            String insertQuery = "INSERT INTO " + tableDestination + " (" + String.join(", ", columnas) + ") VALUES (" +
                    String.join(", ", Collections.nCopies(columnCount, "?")) + ")";

            try (PreparedStatement pstmt = destinationConnection.prepareStatement(insertQuery)) {
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        Object value = rs.getObject(i);
                        if (value instanceof String) {
                            value = ((String) value).toUpperCase();
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
