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
                    
                    // Selección de datos origen
                    System.out.println("\n=== CONFIGURACIÓN DE ORIGEN ===");
                    String query = configurarOrigen(scanner, sourceConnection);
                    
                    // Selección de tabla destino
                    System.out.println("\n=== CONFIGURACIÓN DE DESTINO ===");
                    String tableDestination = seleccionarTablaDestino(scanner, destinationConnection);

                    // Proceso ETL
                    System.out.println("\n=== TRANSFORMACIÓN ===");
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

    private static String configurarOrigen(Scanner scanner, Connection connection) throws SQLException {
        System.out.println("Seleccione el método de extracción:");
        System.out.println("1. Ingresar consulta SQL manualmente");
        System.out.println("2. Seleccionar una tabla de la base de datos");
        int opcion = scanner.nextInt();
        scanner.nextLine();

        if (opcion == 1) {
            System.out.print("Ingrese la consulta SQL para extraer los datos: ");
            return scanner.nextLine();
        } else {
            return seleccionarTablaOrigen(scanner, connection);
        }
    }

    private static String seleccionarTablaOrigen(Scanner scanner, Connection connection) throws SQLException {
        List<String> tablaNombres = listarTablasDisponibles(connection, "origen");
        
        System.out.print("Ingrese el nombre de la tabla origen: ");
        String tablaOrigen = scanner.nextLine();
        
        while (!tablaNombres.contains(tablaOrigen)) {
            System.out.println("La tabla '" + tablaOrigen + "' no existe.");
            System.out.print("Por favor ingrese un nombre de tabla válido: ");
            tablaOrigen = scanner.nextLine();
        }

        System.out.println("\nColumnas disponibles en '" + tablaOrigen + "':");
        ResultSet columns = connection.getMetaData().getColumns(null, null, tablaOrigen, "%");
        List<String> columnasDisponibles = new ArrayList<>();
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            System.out.println("- " + columnName);
            columnasDisponibles.add(columnName);
        }

        System.out.print("\nIngrese los nombres de las columnas a exportar (separadas por comas): ");
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

    private static String seleccionarTablaDestino(Scanner scanner, Connection connection) throws SQLException {
        List<String> tablasDestino = listarTablasDisponibles(connection, "destino");
        
        System.out.print("Ingrese el nombre de la tabla de destino: ");
        String tablaDestino = scanner.nextLine();
        
        while (!tablasDestino.contains(tablaDestino)) {
            System.out.println("La tabla '" + tablaDestino + "' no existe en el destino.");
            System.out.print("Por favor ingrese un nombre de tabla válido: ");
            tablaDestino = scanner.nextLine();
        }
        
        return tablaDestino;
    }

    private static List<String> listarTablasDisponibles(Connection connection, String tipo) throws SQLException {
        List<String> tablaNombres = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});
        
        System.out.println("\nTablas disponibles en la base de datos " + tipo + ":");
        while (tables.next()) {
            String tableName = tables.getString("TABLE_NAME");
            System.out.println("- " + tableName);
            tablaNombres.add(tableName);
        }
        
        if (tablaNombres.isEmpty()) {
            System.out.println("No se encontraron tablas.");
        }
        
        return tablaNombres;
    }

    private static void extractTransformLoad(Connection sourceConn, Connection destConn, String query, 
    String tableDestination, Scanner scanner) {
try (Statement stmt = sourceConn.createStatement();
     ResultSet rs = stmt.executeQuery(query)) {

    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();
    List<String> columnasOrigen = new ArrayList<>();

    // Obtener los nombres de las columnas del origen
    for (int i = 1; i <= columnCount; i++) {
        columnasOrigen.add(metaData.getColumnName(i));
    }

    // Obtener columnas del destino
    List<String> columnasDestino = obtenerColumnasDestino(destConn, tableDestination);

    // Mostrar columnas disponibles y permitir mapeo
    System.out.println("\n=== MAPEO DE COLUMNAS ===");
    System.out.println("Columnas origen: " + String.join(", ", columnasOrigen));
    System.out.println("Columnas destino: " + String.join(", ", columnasDestino));
    
    // Crear mapeo entre columnas origen y destino
    Map<Integer, String> mapeoColumnas = new HashMap<>();
    for (int i = 0; i < columnasOrigen.size(); i++) {
        String columnaOrigen = columnasOrigen.get(i);
        System.out.print("A qué columna destino corresponde '" + columnaOrigen + "'? (deje vacío para omitir): ");
        String columnaDestino = scanner.nextLine().trim();
        
        if (columnasDestino.contains(columnaDestino)) {
            mapeoColumnas.put(i, columnaDestino);
        } else if (!columnaDestino.isEmpty()) {
            System.out.println("La columna destino '" + columnaDestino + "' no existe. Se omitirá.");
        }
    }

    // Filtrar columnas que tendrán mapeo
    List<String> columnasOrigenMapeadas = new ArrayList<>();
    List<String> columnasDestinoMapeadas = new ArrayList<>();
    for (int i = 0; i < columnasOrigen.size(); i++) {
        if (mapeoColumnas.containsKey(i)) {
            columnasOrigenMapeadas.add(columnasOrigen.get(i));
            columnasDestinoMapeadas.add(mapeoColumnas.get(i));
        }
    }

    if (columnasOrigenMapeadas.isEmpty()) {
        throw new SQLException("No se ha definido ningún mapeo de columnas válido");
    }

    // Solicitar las transformaciones (usando nombres de origen)
    System.out.println("\n=== TRANSFORMACIONES ===");
    Map<String, String> transformaciones = solicitarTransformaciones(scanner, columnasOrigenMapeadas);

    // Construcción de la consulta
    String sql = construirMergeSQL(tableDestination, columnasOrigenMapeadas, columnasDestinoMapeadas, destConn);

    try (PreparedStatement pstmt = destConn.prepareStatement(sql)) {
        while (rs.next()) {
            // Aplicar transformaciones y ejecutar
            int paramIndex = 1;
            for (int i = 0; i < columnasOrigenMapeadas.size(); i++) {
                String columnaOrigen = columnasOrigenMapeadas.get(i);
                Object valor = aplicarTransformacion(rs.getObject(columnaOrigen), 
                        transformaciones.get(columnaOrigen));
                pstmt.setObject(paramIndex++, valor);
            }

            // Solo necesitamos setear los valores adicionales para MERGE
            if (sql.startsWith("MERGE")) {
                for (int i = 0; i < columnasOrigenMapeadas.size(); i++) {
                    String columnaOrigen = columnasOrigenMapeadas.get(i);
                    Object valor = aplicarTransformacion(rs.getObject(columnaOrigen), 
                            transformaciones.get(columnaOrigen));
                    pstmt.setObject(paramIndex++, valor);
                }
            }
            pstmt.executeUpdate();
        }
        System.out.println("\nProceso ETL completado con éxito. Datos cargados en '" + tableDestination + "'");
    }
} catch (SQLException e) {
    System.out.println("Error en el proceso ETL:");
    e.printStackTrace();
}
}

private static List<String> obtenerColumnasDestino(Connection destConn, String tableDestination) throws SQLException {
    List<String> columnasDestino = new ArrayList<>();
    DatabaseMetaData metaData = destConn.getMetaData();
    try (ResultSet columns = metaData.getColumns(null, null, tableDestination, "%")) {
        while (columns.next()) {
            columnasDestino.add(columns.getString("COLUMN_NAME"));
        }
    }
    return columnasDestino;
}

    private static Map<String, String> solicitarTransformaciones(Scanner scanner, List<String> columnas) {
        Map<String, String> transformaciones = new HashMap<>();
        System.out.println("\nConfiguración de transformaciones:");

        for (String columna : columnas) {
            System.out.println("\nColumna: " + columna);
            System.out.println("1. Convertir a minúsculas");
            System.out.println("2. Convertir a mayúsculas");
            System.out.println("3. Extraer parte de fecha");
            System.out.println("4. Concatenar con otro valor");
            System.out.println("5. Ninguna transformación");
            System.out.print("Seleccione opción: ");

            int opcion = scanner.nextInt();
            scanner.nextLine();

            switch (opcion) {
                case 1:
                    transformaciones.put(columna, "lower");
                    break;
                case 2:
                    transformaciones.put(columna, "upper");
                    break;
                case 3:
                    System.out.print("Parte a extraer (Año/Mes/Día/Hora): ");
                    String parteFecha = scanner.nextLine();
                    transformaciones.put(columna, "date:" + parteFecha);
                    break;
                case 4:
                    System.out.print("Valor a concatenar: ");
                    String valorConcatenar = scanner.nextLine();
                    transformaciones.put(columna, "concat:" + valorConcatenar);
                    break;
                default:
                    transformaciones.put(columna, "none");
                    break;
            }
        }
        return transformaciones;
    }

    private static String construirMergeSQL(String tableDestination, List<String> columnasOrigen, 
    List<String> columnasDestino, Connection destConn) throws SQLException {
    // Primero verificamos si la tabla está vacía
    boolean tablaVacia = isTablaVacia(destConn, tableDestination);

    if (tablaVacia) {
        // Construir INSERT simple optimizado usando columnas destino
        return construirInsertSQL(tableDestination, columnasDestino);
    } else {
        // Construir MERGE como antes, pero mapeando origen a destino
        return construirMergeSQLCompleto(tableDestination, columnasOrigen, columnasDestino);
    }
    }
    private static boolean isTablaVacia(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() && rs.getInt(1) == 0;
        }
    }
    
    private static String construirInsertSQL(String tableDestination, List<String> columnasDestino) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableDestination).append(" (")
           .append(String.join(", ", columnasDestino))
           .append(") VALUES (");
        
        // Añadir placeholders
        for (int i = 0; i < columnasDestino.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append('?');
        }
        
        return sql.append(")").toString();
    }
    
    private static String construirMergeSQLCompleto(String tableDestination, 
        List<String> columnasOrigen, List<String> columnasDestino) {
    // Asumimos que la primera columna es la clave primaria para el MERGE
    // En una implementación real, deberías detectar la clave primaria
    
    StringBuilder sql = new StringBuilder();
    sql.append("MERGE INTO ").append(tableDestination).append(" AS destino ")
       .append("USING (VALUES (");
    
    // Placeholders para los valores de origen
    for (int i = 0; i < columnasOrigen.size(); i++) {
        if (i > 0) sql.append(", ");
        sql.append('?');
    }
    
    sql.append(")) AS origen (")
       .append(String.join(", ", columnasOrigen))
       .append(") ON destino.")
       .append(columnasDestino.get(0))
       .append(" = origen.")
       .append(columnasOrigen.get(0))
       .append(" WHEN MATCHED THEN UPDATE SET ");
    
    // Actualizar solo las columnas que coinciden (empezando desde la segunda)
    boolean first = true;
    for (int i = 1; i < columnasDestino.size(); i++) {
        if (i >= columnasOrigen.size()) break; // No más columnas en origen
        
        if (!first) sql.append(", ");
        sql.append("destino.").append(columnasDestino.get(i))
           .append(" = origen.").append(columnasOrigen.get(i));
        first = false;
    }
    
    sql.append(" WHEN NOT MATCHED THEN INSERT (")
       .append(String.join(", ", columnasDestino))
       .append(") VALUES (");
    
    // Valores para INSERT - usar placeholders para las columnas destino
    first = true;
    for (int i = 0; i < columnasDestino.size(); i++) {
        if (!first) sql.append(", ");
        sql.append('?');
        first = false;
    }
    
    return sql.append(");").toString();
}

    private static void aplicarTransformacionesYEjectuarMerge(ResultSet rs, PreparedStatement pstmt, 
            List<String> columnas, Map<String, String> transformaciones, int columnCount) throws SQLException {
        for (int i = 0; i < columnCount; i++) {
            String columna = columnas.get(i);
            Object valor = aplicarTransformacion(rs.getObject(columna), transformaciones.get(columna));
            pstmt.setObject(i + 1, valor);
            pstmt.setObject(i + 1 + columnCount, valor);
        }
        pstmt.executeUpdate();
    }

    private static Object aplicarTransformacion(Object valor, String transformacion) {
        if (valor == null || "none".equals(transformacion)) {
            return valor;
        }

        switch (transformacion.split(":")[0]) {
            case "lower":
                return valor.toString().toLowerCase();
            case "upper":
                return valor.toString().toUpperCase();
            case "date":
                return extraerParteFecha(valor, transformacion.split(":")[1]);
            case "concat":
                return valor.toString() + transformacion.split(":")[1];
            default:
                return valor;
        }
    }

    private static Object extraerParteFecha(Object valor, String parteFecha) {
        if (!(valor instanceof java.util.Date)) {
            return valor;
        }

        java.util.Calendar calendar = java.util.Calendar.getInstance();
        calendar.setTime((java.util.Date) valor);

        switch (parteFecha.toLowerCase()) {
            case "año":
                return calendar.get(java.util.Calendar.YEAR);
            case "mes":
                return calendar.get(java.util.Calendar.MONTH) + 1;
            case "día":
                return calendar.get(java.util.Calendar.DAY_OF_MONTH);
            case "hora":
                return calendar.get(java.util.Calendar.HOUR_OF_DAY);
            default:
                return valor;
        }
    }
}