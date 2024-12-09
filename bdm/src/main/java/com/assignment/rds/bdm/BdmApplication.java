package com.assignment.rds.bdm;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class BdmApplication {

	private static String DB_URL = "jdbc:mysql://database-assignment-3.cz402as4e47k.ap-southeast-2.rds.amazonaws.com:3306/bdaassignment";
	private static String DB_USER = "admin";
	private static String DB_PASSWORD = "Test-iit-123";

	public static void main(String[] args) {
		Connection connection = null;
		try {
			// Load the JDBC driver
			Class.forName("com.mysql.cj.jdbc.Driver"); // Use "org.postgresql.Driver" for PostgreSQL

			// Establish a connection to the database
			connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

			System.out.println("Available Databases");
			if (connection != null) {
				System.out.println("Connected to the database!");
			} else {
				System.out.println("Failed to connect to the database.");
			}

			create();

//			insert_stock();

//			insert_company();

			String condition = "priceDate < '2022-08-20' OR companyId = (SELECT id FROM company WHERE name = 'GameStop')";
			delete("stockprice", condition);


//			String queryOne = "numEmployees > 10000 OR annualRevenue < 1000000 ORDER BY name ASC;";
//			select("company", new ArrayList<>(), queryOne, "");

			String queryTwo = """
            SELECT 
                c.name AS companyName,
                c.ticker,
                MIN(s.lowPrice) AS lowestPrice,
                MAX(s.highPrice) AS highestPrice,
                ROUND(AVG(s.closePrice), 2) AS avgClosingPrice,
                ROUND(AVG(s.volume), 0) AS avgVolume
            FROM 
                stockprice s
            JOIN 
                company c
            ON 
                s.companyId = c.id
            WHERE 
                s.priceDate BETWEEN '2022-08-22' AND '2022-08-26'
            GROUP BY 
                c.name, c.ticker
            ORDER BY 
                avgVolume DESC;
        """;

//			executeAndPrint(new StringBuilder(queryTwo));

			String queryThree = """
            WITH WeeklyAverages AS (
                SELECT 
                    s.companyId,
                    ROUND(AVG(s.closePrice), 2) AS avgClosePrice
                FROM 
                    stockprice s
                WHERE 
                    s.priceDate BETWEEN '2022-08-15' AND '2022-08-19'
                GROUP BY 
                    s.companyId
            )
            SELECT 
                c.name AS companyName,
                c.ticker,
                s.closePrice AS closePriceOnAug30,
                w.avgClosePrice AS avgClosePrice
            FROM 
                company c
            LEFT JOIN 
                stockprice s
            ON 
                c.id = s.companyId AND s.priceDate = '2022-08-30'
            LEFT JOIN 
                WeeklyAverages w
            ON 
                c.id = w.companyId
            WHERE 
                (s.closePrice IS NULL OR s.closePrice >= w.avgClosePrice * 0.9 OR c.ticker IS NULL)
            ORDER BY 
                c.name ASC;
        """;

			executeAndPrint(new StringBuilder(queryThree));

		} catch (ClassNotFoundException e) {
			System.err.println("JDBC Driver not found. Ensure it is added to the classpath.");
			e.printStackTrace();
		} catch (SQLException e) {
			System.err.println("Error while connecting to the database.");
			e.printStackTrace();
		} finally {
			// Close the connection
			if (connection != null) {
				try {
					connection.close();
					System.out.println("Database connection closed.");
				} catch (SQLException e) {
					System.err.println("Error while closing the connection.");
					e.printStackTrace();
				}
			}
		}
	}


	public static void create() {// Explicitly select the database
		String createCompanyTableSQL = "CREATE TABLE company ("
				+ "id INT PRIMARY KEY, "
				+ "name VARCHAR(50), "
				+ "ticker CHAR(10), "
				+ "annualRevenue DECIMAL(15, 2), "
				+ "numEmployees INT"
				+ ");";

		String createStockPriceTableSQL = "CREATE TABLE stockprice ("
				+ "companyId INT, "
				+ "priceDate DATE, "
				+ "openPrice DECIMAL(10, 2), "
				+ "highPrice DECIMAL(10, 2), "
				+ "lowPrice DECIMAL(10, 2), "
				+ "closePrice DECIMAL(10, 2), "
				+ "volume INT, "
				+ "PRIMARY KEY (companyId, priceDate), "
				+ "FOREIGN KEY (companyId) REFERENCES company(id)"
				+ ");";

		// Establish a connection and create tables
		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			 Statement stmt = conn.createStatement()) {

			// Execute SQL statements to create tables
			stmt.executeUpdate(createCompanyTableSQL);
			stmt.executeUpdate(createStockPriceTableSQL);

			System.out.println("Tables created successfully!");

		} catch (SQLException e) {
			System.out.println("An error occurred while creating the tables.");
			e.printStackTrace();
		}
	}

	public static void insert(String tableName, String[] columns, Object[][] data) {
		// Build SQL query dynamically
		StringBuilder query = new StringBuilder("INSERT INTO " + tableName + " (");
		for (String column : columns) {
			query.append(column).append(", ");
		}
		query.delete(query.length() - 2, query.length()).append(") VALUES (");
		for (int i = 0; i < columns.length; i++) {
			query.append("?, ");
		}
		query.delete(query.length() - 2, query.length()).append(")");

		// Insert data
		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			 PreparedStatement pstmt = conn.prepareStatement(query.toString())) {

			for (Object[] row : data) {
				for (int i = 0; i < row.length; i++) {
					pstmt.setObject(i + 1, row[i]); // Set each value dynamically
				}
				pstmt.executeUpdate();
			}

			System.out.println("Data inserted successfully into " + tableName);

		} catch (SQLException e) {
			System.out.println("An error occurred while inserting data into " + tableName);
			e.printStackTrace();
		}
	}


	public static void insert_company() {
		String[] columns = {"id", "name", "ticker", "annualRevenue", "numEmployees"};

		Object[][] data = {
				{1, "Apple", "AAPL", 387540000000.00, 154000},
				{2, "GameStop", "GME", 611000000.00, 12000},
				{3, "Handy Repair", null, 2000000.00, 50},
				{4, "Microsoft", "MSFT", 198270000000.00, 221000},
				{5, "StartUp", null, 50000.00, 3}
		};

//		insert("company", columns, data);

		select("company", List.of(columns), "", "");
	}

	public static void insert_stock() {
		String[] columns = {"companyId", "priceDate", "openPrice", "highPrice", "lowPrice", "closePrice", "volume"};
		Object[][] data = {
				{1, "2022-08-15", 171.52, 173.39, 171.35, 173.19, 54091700},
				{1, "2022-08-16", 172.78, 173.71, 171.66, 173.03, 56377100},
				{1, "2022-08-17", 172.77, 176.15, 172.57, 174.55, 79542000},
				{1, "2022-08-18", 173.75, 174.90, 173.12, 174.15, 62290100},
				{1, "2022-08-19", 173.03, 173.74, 171.31, 171.52, 70211500},
				{1, "2022-08-22", 169.69, 169.86, 167.14, 167.57, 69026800},
				{1, "2022-08-23", 167.08, 168.71, 166.65, 167.23, 54147100},
				{1, "2022-08-24", 167.32, 168.11, 166.25, 167.53, 53841500},
				{1, "2022-08-25", 168.78, 170.14, 168.35, 170.03, 51218200},
				{1, "2022-08-26", 170.57, 171.05, 163.56, 163.62, 78823500},
				{1, "2022-08-29", 161.15, 162.90, 159.82, 161.38, 73314000},
				{1, "2022-08-30", 162.13, 162.56, 157.72, 158.91, 77906200},
				{2, "2022-08-15", 39.75, 40.39, 38.81, 39.68, 5243100},
				{2, "2022-08-16", 39.17, 45.53, 38.60, 42.19, 23602800},
				{2, "2022-08-17", 42.18, 44.36, 40.41, 40.52, 9766400},
				{2, "2022-08-18", 39.27, 40.07, 37.34, 37.93, 8145400},
				{2, "2022-08-19", 35.18, 37.19, 34.67, 36.49, 9525600},
				{2, "2022-08-22", 34.31, 36.20, 34.20, 34.50, 5798600},
				{2, "2022-08-23", 34.70, 34.99, 33.45, 33.53, 4836300},
				{2, "2022-08-24", 34.00, 34.94, 32.44, 32.50, 5620300},
				{2, "2022-08-25", 32.84, 32.89, 31.50, 31.96, 4726300},
				{2, "2022-08-26", 31.50, 32.38, 30.63, 30.94, 4289500},
				{2, "2022-08-29", 30.48, 32.75, 30.38, 31.55, 4292700},
				{2, "2022-08-30", 31.62, 31.87, 29.42, 29.84, 5060200},
				{4, "2022-08-15", 291.00, 294.18, 290.11, 293.47, 18085700},
				{4, "2022-08-16", 291.99, 294.04, 290.42, 292.71, 18102900},
				{4, "2022-08-17", 289.74, 293.35, 289.47, 291.32, 18253400},
				{4, "2022-08-18", 290.19, 291.91, 289.08, 290.17, 17186200},
				{4, "2022-08-19", 288.90, 289.25, 285.56, 286.15, 20557200},
				{4, "2022-08-22", 282.08, 282.46, 277.22, 277.75, 25061100},
				{4, "2022-08-23", 276.44, 278.86, 275.40, 276.44, 17527400},
				{4, "2022-08-24", 275.41, 277.23, 275.11, 275.79, 18137000},
				{4, "2022-08-25", 277.33, 279.02, 274.52, 278.85, 16583400},
				{4, "2022-08-26", 279.08, 280.34, 267.98, 268.09, 27532500},
				{4, "2022-08-29", 265.85, 267.40, 263.85, 265.23, 20338500},
				{4, "2022-08-30", 266.67, 267.05, 260.66, 262.97, 22767100},
		};

		insert("stockprice", columns, data);

		select("stockprice", List.of(columns), "", "");
	}

	public static void delete(String tableName, String condition) {
		String query = "DELETE FROM " + tableName + " WHERE " + condition;

		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			 PreparedStatement pstmt = conn.prepareStatement(query)) {

			int rowsAffected = pstmt.executeUpdate();
			System.out.println("Deleted " + rowsAffected + " rows from " + tableName);

		} catch (Exception e) {
			System.out.println("An error occurred while deleting records.");
			e.printStackTrace();
		}
	}


	public static void select(String tableName, List<String> columns, String whereClause, String orderBy) {
		// Build SQL query dynamically
		StringBuilder query = new StringBuilder("SELECT ");
		if (columns == null || columns.isEmpty()) {
			query.append("*"); // Select all columns if none are specified
		} else {
			query.append(String.join(", ", columns));
		}
		query.append(" FROM ").append(tableName);

		if (whereClause != null && !whereClause.isEmpty()) {
			query.append(" WHERE ").append(whereClause);
		}

		if (orderBy != null && !orderBy.isEmpty()) {
			query.append(" ORDER BY ").append(orderBy);
		}

		// Execute the query
		executeAndPrint(query);
	}

	private static void executeAndPrint(StringBuilder query) {
		int maxrows = 100;
		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			 Statement stmt = conn.createStatement();
			 ResultSet rst = stmt.executeQuery(query.toString())) {

			StringBuffer buf = new StringBuffer(5000);
			int rowCount = 0;
			if (rst == null)
				System.out.println("ERROR: No ResultSet");
			ResultSetMetaData meta = rst.getMetaData();
			buf.append("Total columns: ").append(meta.getColumnCount());
			buf.append('\n');
			if (meta.getColumnCount() > 0)
				buf.append(meta.getColumnName(1));
			for (int j = 2; j <= meta.getColumnCount(); j++)
				buf.append(", " + meta.getColumnName(j));
			buf.append('\n');
			while (rst.next())
			{
				if (rowCount < maxrows)
				{
					for (int j = 0; j < meta.getColumnCount(); j++)
					{
						Object obj = rst.getObject(j + 1);
						buf.append(obj);
						if (j != meta.getColumnCount() - 1)
							buf.append(", ");
					}
					buf.append('\n');
				}
				rowCount++;
			}
			buf.append("Total results: ").append(rowCount);
			System.out.println(buf);

		} catch (SQLException e) {
			System.out.println("An error occurred while executing the query.");
			e.printStackTrace();
		}
	}

}
