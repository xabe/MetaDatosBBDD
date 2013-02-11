package com.xabe;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Main {

	
	private static Connection getConnectionMysql(String url,String user,String pass) throws SQLException,ClassNotFoundException{
		Class.forName("com.mysql.jdbc.Driver");
		return  DriverManager.getConnection(url, user, pass);
	}
	
	private static Connection getConnectionOracle(String url,String user,String pass) throws SQLException,ClassNotFoundException{
		Class.forName("oracle.jdbc.OracleDriver");
		return  DriverManager.getConnection(url, user, pass);
	}
	
	private static Connection getConnectionSqlServer(String url,String user,String pass) throws SQLException,ClassNotFoundException{
		Class.forName("net.sourceforge.jtds.jdbc.Driver");
		return  DriverManager.getConnection(url, user, pass);
	}

	public static void main(String[] args) {
		try
		{
			Connection conexion = null;
			switch (1) {
			case 1: conexion = getConnectionMysql("jdbc:mysql://localhost:3306/kodeengine", "root", "root");
				break;
			case 2: conexion = getConnectionOracle("jdbc:oracle:thin:@localhost:1521:XE", "ITS", "ITS");
				break;
			case 3: conexion = getConnectionSqlServer("jdbc:jtds:sqlserver://:1433;databaseName=SZENA_SMART_DATA_DB", "", "");
				break;
			}

			DatabaseMetaData metaDatos = conexion.getMetaData();
			/**
			 * catálogo de la base de datos. Al poner null, estamos preguntando por el catálogo actual, que en nuestro ejemplo es de la cadena de conexión, "kodeengine".
			 * Esquema de la base de datos. Al poner null, es el actual.
			 * Patrón para las tablas en las que tenemos interés. En SQL el caracter que indica "todo" es %, equivalente al * a la hora de listar ficheros. Esto nos dará todas las tablas del catálogo y esquema actual. 
			 * Podríamos poner cosas como "person%", con lo que obtendríamos todas las tablas cuyo nombre empiece por "person". 
			 * El cuarto parámetro es un array de String, en el que pondríamos qué tipos de tablas queremos (normales, vistas, etc). Al poner null, nos devolverá todos los tipos de tablas
			 */
			ResultSet rs = metaDatos.getTables(null, null, "%", null);
			ResultSet table;
			ResultSet primaryKey;
			
			System.out.println("Conectado con el driver "+metaDatos.getDriverName()+" "+metaDatos.getDriverVersion());
			System.out.println("Conectado a la base de datos : "+metaDatos.getDatabaseProductName()+" Version del producto : "+metaDatos.getDatabaseProductVersion());
			System.out.println("TABLAS");
			while (rs.next()) {
				   // El contenido de cada columna del ResultSet se puede ver
				   // en la API, en el metodo getTables() de DataBaseMetaData.
				   String catalogo = rs.getString(1);
				   String schema = rs.getString(2);
				   String tabla = rs.getString(3);
				   String typeTable = rs.getString(4);
				   System.out.println("\t -> TABLA = " + catalogo == null ? schema : catalogo + "." + tabla+" Tipos de tablas : "+typeTable);
				   table = metaDatos.getColumns(catalogo, schema, tabla, null);
				   primaryKey = metaDatos.getPrimaryKeys(catalogo, schema, tabla);
				   while (table.next()) {
				      // El contenido de cada columna del ResultSet se puede ver en
				      // la API de java, en el metodo getColumns() de DataBaseMetaData
				      String nombreColumna = table.getString(4);
				      String tipoColumna = table.getString(6);
				      int tamayoColumna = table.getInt(7);
				      String isNull = table.getString(18);
				      String isAutoIncrement = "";
				      try
				      {
				    	  isAutoIncrement = table.getString(23);
				      }catch(SQLException exception){
				    	  isAutoIncrement = "false";
				      }
				      System.out.println("\t\t -> COLUMNA, nombre=" + nombreColumna);
				      System.out.println("\t\t\t -> tipo = " + tipoColumna);
				      System.out.println("\t\t\t -> tamaño de la columna = " + tamayoColumna);
				      System.out.println("\t\t\t -> Si es nulo = " + isNull);
				      System.out.println("\t\t\t -> Si es auto incremental = " + isAutoIncrement);
				   }
				   System.out.println("\t -> Primary key de la tabla es = " + catalogo == null ? schema : catalogo + "." + tabla);
				   boolean tienePrimaryKey = false;
				   while (primaryKey.next()) {
					  String columnName = primaryKey.getString(4);
					  tienePrimaryKey = true;
				      System.out.println("\t\t\t -> Primray Key = " + columnName);
				   }
				   if(!tienePrimaryKey)
				   {
					   System.out.println("\t\t\t -> No tiene primray Key");
				   }
				   System.out.println();
				   table.close();
				   primaryKey.close();
			}
			rs.close();
			conexion.close();
			
		}catch(Exception exception){
			exception.printStackTrace();
		}
	}

}
