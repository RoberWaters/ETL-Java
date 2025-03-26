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

                    extractTransformLoad(sourceConnection, destinationConnection, query, tableDestination, scanner);
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

    private static void extractTransformLoad(Connection sourceConn, Connection destConn, String query, String tableDestination, Scanner scanner) {
        try (Statement stmt = sourceConn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
    
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> columnas = new ArrayList<>();
    
            // Obtener los nombres de las columnas
            for (int i = 1; i <= columnCount; i++) {
                columnas.add(metaData.getColumnName(i));
            }
    
            // Solicitar las transformaciones
            Map<String, String> transformaciones = new HashMap<>();
            for (String columna : columnas) {
                System.out.println("Seleccione la transformación para la columna " + columna + ":");
                System.out.println("1. Convertir a minúsculas");
                System.out.println("2. Convertir a mayúsculas");
                System.out.println("3. Extraer parte de fecha (Año, Mes, Día, Hora)");
                System.out.println("4. Concatenar con otro valor");
                System.out.println("5. Ninguna transformación");
    
                int opcion = scanner.nextInt();
                scanner.nextLine();  // Limpiar el buffer del scanner
    
                switch (opcion) {
                    case 1:
                        transformaciones.put(columna, "lower");
                        break;
                    case 2:
                        transformaciones.put(columna, "upper");
                        break;
                    case 3:
                        System.out.println("Seleccione la parte de la fecha: (Año, Mes, Día, Hora)");
                        String parteFecha = scanner.nextLine();
                        transformaciones.put(columna, "date:" + parteFecha);
                        break;
                    case 4:
                        System.out.print("Ingrese el valor con el que desea concatenar: ");
                        String valorConcatenar = scanner.nextLine();
                        transformaciones.put(columna, "concat:" + valorConcatenar);
                        break;
                    case 5:
                        transformaciones.put(columna, "none");
                        break;
                    default:
                        System.out.println("Opción no válida. No se realizará transformación.");
                        transformaciones.put(columna, "none");
                        break;
                }
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
                        String columna = columnas.get(i);
                        Object valor = rs.getObject(columna);
    
                        // Aplicar las transformaciones
                        if ("lower".equals(transformaciones.get(columna))) {
                            valor = valor.toString().toLowerCase();
                        } else if ("upper".equals(transformaciones.get(columna))) {
                            valor = valor.toString().toUpperCase();
                        } else if (transformaciones.get(columna).startsWith("date:")) {
                            // Extraer parte de la fecha (Año, Mes, Día, Hora)
                            String tipoFecha = transformaciones.get(columna).split(":")[1];
                            if (valor instanceof java.sql.Date) {
                                java.sql.Date date = (java.sql.Date) valor;
                                java.util.Calendar calendar = java.util.Calendar.getInstance();
                                calendar.setTime(date);
                                switch (tipoFecha.toLowerCase()) {
                                    case "año":
                                        valor = calendar.get(java.util.Calendar.YEAR);
                                        break;
                                    case "mes":
                                        valor = calendar.get(java.util.Calendar.MONTH) + 1; // Mes en base 1
                                        break;
                                    case "día":
                                        valor = calendar.get(java.util.Calendar.DAY_OF_MONTH);
                                        break;
                                    case "hora":
                                        valor = calendar.get(java.util.Calendar.HOUR_OF_DAY);
                                        break;
                                    default:
                                        System.out.println("Transformación de fecha no válida.");
                                        break;
                                }
                            }
                        } else if (transformaciones.get(columna).startsWith("concat:")) {
                            // Concatenar valor con otro valor
                            String valorConcatenar = transformaciones.get(columna).split(":")[1];
                            valor = valor.toString() + valorConcatenar;
                        }
    
                        pstmt.setObject(i + 1, valor);
                        pstmt.setObject(i + 1 + columnCount, valor); // Para el INSERT
                    }
                    pstmt.executeUpdate();
                }
                System.out.println("Proceso ETL completado con éxito.");
            }
        } catch (SQLException e) {
            System.out.println("Error en el proceso ETL:");
            e.printStackTrace();
        }
    }
}     