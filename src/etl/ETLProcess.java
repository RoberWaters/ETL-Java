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

    // Obtener nombres de columnas origen
    for (int i = 1; i <= columnCount; i++) {
        columnasOrigen.add(metaData.getColumnName(i));
    }

    // Obtener estructura tabla destino
    List<String> columnasDestino = obtenerColumnasDestino(destConn, tableDestination);
    List<String> primaryKeys = obtenerClavesPrimarias(destConn, tableDestination);

    // Mapeo de columnas
    Map<String, String> mapeoColumnas = obtenerMapeoColumnas(scanner, columnasOrigen, columnasDestino, primaryKeys);
    
    // Filtrar columnas mapeadas
    List<String> columnasOrigenMapeadas = new ArrayList<>();
    List<String> columnasDestinoMapeadas = new ArrayList<>();
    for (int i = 0; i < columnasOrigen.size(); i++) {
        if (mapeoColumnas.containsKey(columnasOrigen.get(i))) {
            columnasOrigenMapeadas.add(columnasOrigen.get(i));
            columnasDestinoMapeadas.add(mapeoColumnas.get(columnasOrigen.get(i)));
        }
    }

    // Transformaciones
    Map<String, String> transformaciones = solicitarTransformaciones(scanner, columnasOrigenMapeadas);

    // Construir consulta SQL adecuada
    String sql;
    int totalParametros;
    if (!primaryKeys.isEmpty()) {
        sql = construirUpsertSQL(tableDestination, columnasOrigenMapeadas, columnasDestinoMapeadas, primaryKeys);
        // Parámetros: valores para UPDATE + valores para WHERE (PKs) + valores para INSERT (si es necesario)
        totalParametros = (columnasDestinoMapeadas.size() - primaryKeys.size()) + primaryKeys.size() + columnasDestinoMapeadas.size();
    } else {
        sql = construirInsertConVerificacion(tableDestination, columnasDestinoMapeadas);
        // Parámetros: valores para WHERE + valores para INSERT
        totalParametros = columnasDestinoMapeadas.size() * 2;
    }

    try (PreparedStatement pstmt = destConn.prepareStatement(sql)) {
        int batchSize = 0;
        final int MAX_BATCH_SIZE = 1000;
        destConn.setAutoCommit(false);

        try {
            while (rs.next()) {
                int paramIndex = 1;
                Object[] valores = new Object[totalParametros];
                int valoresIndex = 0;

                // Obtener y transformar todos los valores primero
                for (String columnaOrigen : columnasOrigenMapeadas) {
                    valores[valoresIndex++] = aplicarTransformacion(rs.getObject(columnaOrigen), 
                            transformaciones.get(columnaOrigen));
                }

                // Lógica para establecer parámetros según el tipo de consulta
                if (!primaryKeys.isEmpty()) {
                    // Consulta UPSERT (UPDATE + INSERT)
                    
                    // 1. Parámetros para UPDATE (todos menos PKs)
                    for (int i = 0; i < columnasDestinoMapeadas.size(); i++) {
                        if (!primaryKeys.contains(columnasDestinoMapeadas.get(i))) {
                            pstmt.setObject(paramIndex++, valores[i]);
                        }
                    }
                    
                    // 2. Parámetros para WHERE (solo PKs)
                    for (String pk : primaryKeys) {
                        int index = columnasDestinoMapeadas.indexOf(pk);
                        pstmt.setObject(paramIndex++, valores[index]);
                    }
                    
                    // 3. Parámetros para INSERT (todos)
                    for (int i = 0; i < columnasDestinoMapeadas.size(); i++) {
                        pstmt.setObject(paramIndex++, valores[i]);
                    }
                } else {
                    // Consulta INSERT con verificación
                    
                    // 1. Parámetros para WHERE (todos)
                    for (int i = 0; i < columnasDestinoMapeadas.size(); i++) {
                        pstmt.setObject(paramIndex++, valores[i]);
                    }
                    
                    // 2. Parámetros para INSERT (todos)
                    for (int i = 0; i < columnasDestinoMapeadas.size(); i++) {
                        pstmt.setObject(paramIndex++, valores[i]);
                    }
                }

                pstmt.addBatch();
                batchSize++;
                
                if (batchSize % MAX_BATCH_SIZE == 0) {
                    pstmt.executeBatch();
                    destConn.commit();
                    System.out.println("Procesados " + batchSize + " registros...");
                }
            }
            
            if (batchSize % MAX_BATCH_SIZE != 0) {
                pstmt.executeBatch();
                destConn.commit();
            }
            
            System.out.println("\nProceso ETL completado con éxito. Datos cargados en '" + tableDestination + "'");
        } catch (SQLException e) {
            destConn.rollback();
            throw e;
        } finally {
            destConn.setAutoCommit(true);
        }
    }
} catch (SQLException e) {
    System.out.println("Error en el proceso ETL:");
    e.printStackTrace();
}
}


    private static List<String> obtenerClavesPrimarias(Connection conn, String tableName) throws SQLException {
        List<String> primaryKeys = new ArrayList<>();
        DatabaseMetaData metaData = conn.getMetaData();
        
        try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
            while (rs.next()) {
                primaryKeys.add(rs.getString("COLUMN_NAME"));
            }
        }
        return primaryKeys;
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

    private static Map<String, String> obtenerMapeoColumnas(Scanner scanner, List<String> columnasOrigen, 
    List<String> columnasDestino, List<String> primaryKeys) {
Map<String, String> mapeo = new HashMap<>();

System.out.println("\n=== MAPEO DE COLUMNAS ===");
System.out.println("Columnas origen: " + String.join(", ", columnasOrigen));
System.out.println("Columnas destino: " + String.join(", ", columnasDestino));
if (!primaryKeys.isEmpty()) {
    System.out.println("Claves primarias detectadas: " + String.join(", ", primaryKeys));
}

for (String columnaOrigen : columnasOrigen) {
    System.out.print("A qué columna destino corresponde '" + columnaOrigen + "'? (deje vacío para omitir): ");
    String columnaDestino = scanner.nextLine().trim();
    
    if (columnasDestino.contains(columnaDestino)) {
        mapeo.put(columnaOrigen, columnaDestino);
    } else if (!columnaDestino.isEmpty()) {
        System.out.println("La columna destino '" + columnaDestino + "' no existe. Se omitirá.");
    }
}

// Validar que todas las PK estén mapeadas
if (!primaryKeys.isEmpty()) {
    for (String pk : primaryKeys) {
        if (!mapeo.containsValue(pk)) {
            throw new IllegalArgumentException("La clave primaria '" + pk + "' no está mapeada. Debe mapear todas las claves primarias.");
        }
    }
}

return mapeo;
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

    private static String construirUpsertSQL(String tableName, 
    List<String> columnasOrigen, List<String> columnasDestino, List<String> primaryKeys) {

StringBuilder sql = new StringBuilder();

// 1. Parte UPDATE
sql.append("UPDATE ").append(tableName).append(" SET ");

boolean first = true;
for (String colDest : columnasDestino) {
    if (!primaryKeys.contains(colDest)) {
        if (!first) sql.append(", ");
        sql.append(colDest).append(" = ?");
        first = false;
    }
}

// 2. Condición WHERE con PKs
sql.append(" WHERE ");
first = true;
for (String pk : primaryKeys) {
    if (!first) sql.append(" AND ");
    sql.append(pk).append(" = ?");
    first = false;
}

// 3. Parte INSERT si no se actualizó
sql.append("; IF @@ROWCOUNT = 0 BEGIN INSERT INTO ").append(tableName).append(" (")
   .append(String.join(", ", columnasDestino))
   .append(") VALUES (");

for (int i = 0; i < columnasDestino.size(); i++) {
    if (i > 0) sql.append(", ");
    sql.append("?");
}

sql.append(") END");

return sql.toString();
}

    private static String construirMergeSQL(String tableDestination, List<String> columnasOrigen, 
            List<String> columnasDestino, Connection destConn) throws SQLException {
        
        // Obtener la clave primaria de la tabla destino
        List<String> primaryKeys = obtenerClavesPrimarias(destConn, tableDestination);
        
        if (primaryKeys.isEmpty()) {
            // Si no hay PK, usar INSERT con verificación de existencia para todas las columnas
            return construirInsertConVerificacion(tableDestination, columnasDestino);
        } else {
            // Construir MERGE usando las claves primarias reales
            return construirMergeSQLCompleto(tableDestination, columnasOrigen, columnasDestino, primaryKeys);
        }
    }

    private static String construirInsertConVerificacion(String tableName, List<String> columnasDestino) {
        StringBuilder sql = new StringBuilder();
        
        sql.append("IF NOT EXISTS (SELECT 1 FROM ").append(tableName).append(" WHERE ");
        
        for (int i = 0; i < columnasDestino.size(); i++) {
            if (i > 0) sql.append(" AND ");
            sql.append(columnasDestino.get(i)).append(" = ?");
        }
        
        sql.append(") BEGIN INSERT INTO ").append(tableName).append(" (")
           .append(String.join(", ", columnasDestino))
           .append(") VALUES (");
        
        for (int i = 0; i < columnasDestino.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        
        sql.append(") END");
        
        return sql.toString();
    }
    private static String construirMergeSQLCompleto(String tableDestination, 
    List<String> columnasOrigen, List<String> columnasDestino, List<String> primaryKeys) {

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
    .append(") ON ");

    // Condición de coincidencia basada en todas las claves primarias
    boolean firstCondition = true;
    for (String pk : primaryKeys) {
        int index = columnasDestino.indexOf(pk);
        if (index >= 0 && index < columnasOrigen.size()) {
            if (!firstCondition) sql.append(" AND ");
            sql.append("destino.").append(pk)
            .append(" = origen.").append(columnasOrigen.get(index));
            firstCondition = false;
        }
    }

    // Si no se pudo mapear ninguna PK, lanzar excepción
    if (firstCondition) {
        throw new IllegalArgumentException("No se pudo mapear ninguna clave primaria para la condición MERGE");
    }

    sql.append(" WHEN MATCHED THEN UPDATE SET ");

    // Actualizar solo las columnas que no son PK
    boolean firstUpdate = true;
    for (int i = 0; i < columnasDestino.size(); i++) {
        String colDest = columnasDestino.get(i);
        if (!primaryKeys.contains(colDest)) {
            if (!firstUpdate) sql.append(", ");
            sql.append("destino.").append(colDest)
            .append(" = origen.").append(columnasOrigen.get(i));
            firstUpdate = false;
        }
    }

    sql.append(" WHEN NOT MATCHED THEN INSERT (")
    .append(String.join(", ", columnasDestino))
    .append(") VALUES (");

    // Valores para INSERT (usar nombres de columnas origen)
    for (int i = 0; i < columnasDestino.size(); i++) {
        if (i > 0) sql.append(", ");
        sql.append("origen.").append(columnasOrigen.get(i));
    }

    return sql.append(");").toString();
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