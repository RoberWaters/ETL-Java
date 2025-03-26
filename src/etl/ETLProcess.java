package src.etl;

import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class ETLProcess {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        
        try {
            Properties properties = loadProperties();
            
            // Obtener credenciales de las bases de datos
            String dbUrlOrigen = properties.getProperty("db.origen.url");
            String dbUserOrigen = properties.getProperty("db.origen.user");
            String dbPasswordOrigen = properties.getProperty("db.origen.password");
            
            String dbUrlDestino = properties.getProperty("db.destino.url");
            String dbUserDestino = properties.getProperty("db.destino.user");
            String dbPasswordDestino = properties.getProperty("db.destino.password");

            try (
                Connection sourceConnection = connectToDatabase(dbUrlOrigen, dbUserOrigen, dbPasswordOrigen);
                Connection destinationConnection = connectToDatabase(dbUrlDestino, dbUserDestino, dbPasswordDestino)
            ) {
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
            }

        } catch (Exception e) {
            System.out.println("Error en la ejecución del ETL:");
            e.printStackTrace();
        } finally {
            scanner.close();
        }
    }

    private static Properties loadProperties() throws Exception {
        Properties properties = new Properties();
        try (InputStream input = ETLProcess.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new Exception("El archivo config.properties no se encuentra en el classpath.");
            }
            properties.load(input);
        }
        return properties;
    }

    private static Connection connectToDatabase(String url, String user, String password) {
        try {
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

    private static void extractTransformLoad(Connection sourceConn, Connection destConn, String query, String tableDestination) {
        try (Statement stmt = sourceConn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> columnas = new ArrayList<>();
            
            for (int i = 1; i <= columnCount; i++) {
                columnas.add(metaData.getColumnName(i));
            }

            // Construcción de la consulta MERGE
            String mergeSQL = "MERGE INTO " + tableDestination + " AS destino " +
                    "USING (VALUES (" + String.join(", ", Collections.nCopies(columnCount, "?")) + ")) " +
                    "AS origen (" + String.join(", ", columnas) + ") " +
                    "ON destino." + columnas.get(0) + " = origen." + columnas.get(0) + " " +
                    "WHEN MATCHED THEN UPDATE SET " + 
                    columnas.stream().skip(1).map(c -> "destino." + c + " = origen." + c).collect(Collectors.joining(", ")) + " " +
                    "WHEN NOT MATCHED THEN INSERT (" + String.join(", ", columnas) + ") " +
                    "VALUES (" + String.join(", ", Collections.nCopies(columnCount, "?")) + ");";

            try (PreparedStatement pstmt = destConn.prepareStatement(mergeSQL)) {
                while (rs.next()) {
                    for (int i = 0; i < columnCount; i++) {
                        pstmt.setObject(i + 1, rs.getObject(columnas.get(i)));
                        pstmt.setObject(i + 1 + columnCount, rs.getObject(columnas.get(i))); // Para el INSERT
                    }
                    pstmt.executeUpdate();
                }
                System.out.println("Proceso ETL completado con éxito usando MERGE.");
            }
        } catch (SQLException e) {
            System.out.println("Error en el proceso ETL:");
            e.printStackTrace();
        }
    }
}