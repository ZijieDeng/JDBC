package JDBC;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException;
import com.mysql.jdbc.exceptions.MySQLSyntaxErrorException;
/**
 * @author Zijie Deng 邓子洁 ^_^
 */
public class P2A {
	public static void main(String[] args) throws Exception {
		// Load and register a JDBC driver
		try {
			// Load the driver (registers itself)
			Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception E) {
			System.err.println("Unable to load driver.");
			E.printStackTrace();
		}
		try {
			// Connect to the database
			Connection connection;
			String dbUrl = "jdbc:mysql://csdb.cs.iastate.edu:3306/db363dengzj";
			String user = "dbu363dengzj";
			String password = "nZPrzBVdDKm";
			connection = DriverManager.getConnection(dbUrl, user, password);
			System.out.println("*** Connected to the database ***");

			// Create Statement and ResultSet variables to use throughout the
			// project
			Statement statement = connection.createStatement();
			ResultSet rs;
			{ // Part A
				System.out.println("\nPart A\n");

				String query = "";
				query += "SELECT\n";
				query += "p.Name, i.Salary\n";
				query += "FROM\n";
				query += "Person p,\n";
				query += "Instructor i\n";
				query += "WHERE\n";
				query += "p.ID = i.InstructorID;";

				// get salaries of all instructors
				// rs = statement.executeQuery("select * from Instructor f");
				rs = statement.executeQuery(query);

				int totalSalary = 0;
				int salary;

				while (rs.next()) {
					// get value of salary from each tuple
					salary = rs.getInt("Salary");
					System.out.println(rs.getString("Name") + "\t" + salary);
					totalSalary += salary;
				}
				String report = "Total Salary of all faculty: " + totalSalary;
				System.out.println(String.format("%" + report.length() + "s", "").replace(" ", "="));
				System.out.println(report);
			} // End of Part A

			{ // Part B
				System.out.println("\nPart B\n");

				String queryMeritList = "";
				queryMeritList += "CREATE TABLE MeritList (\n";
				queryMeritList += "StudentID CHAR(9) NOT NULL,\n";
				queryMeritList += "Classification VARCHAR(10),\n";
				queryMeritList += "MentorID CHAR(9),\n";
				queryMeritList += "GPA DOUBLE,\n";
				queryMeritList += "PRIMARY KEY (StudentID)\n";
				queryMeritList += ");";

				PreparedStatement ps = connection.prepareStatement(queryMeritList);
				try {
					ps.execute();
				} catch (MySQLSyntaxErrorException e) {
					// System.err.println(e.getMessage());
				}

				String query = "";
				query += "SELECT\n";
				query += "StudentID, Classification, MentorID, GPA\n";
				query += "FROM\n";
				query += "Student\n";
				query += "GROUP BY GPA DESC;";

				rs = statement.executeQuery(query);
				int count = 0;
				double num = 0;
				while (rs.next()) {
					if (count == 20)
						num = rs.getDouble("GPA");
					else if (count > 20 && rs.getDouble("GPA") != num)
						break;
					count++;

					query = "";
					query += "insert into MeritList ";
					// query += "values (";
					// query += "\'" + rs.getString("StudentID") + "\', ";
					// query += "\'" + rs.getString("Classification") + "\', ";
					// query += "\'" + rs.getString("MentorID") + "\', ";
					// query += "\'" + rs.getDouble("GPA") + "\'";
					// query += ");";
					query += "values (?, ?, ?, ?);";
					ps = connection.prepareStatement(query);
					ps.setString(1, rs.getString("StudentID"));
					ps.setString(2, rs.getString("Classification"));
					ps.setString(3, rs.getString("MentorID"));
					ps.setDouble(4, rs.getDouble("GPA"));
					try {
						ps.execute();
					} catch (MySQLIntegrityConstraintViolationException e) {
						// System.err.println(e.getMessage());
					}
				}
				System.out.println("MeritList created.\n");
			} // End of Part B

			{ // Part C
				System.out.println("Part C\n");

				String query = "SELECT * FROM MeritList ORDER BY GPA desc";
				rs = statement.executeQuery(query);
				while (rs.next()) {
					System.out.println(String.format("%10s", rs.getString("StudentID")) + "\t"
							+ String.format("%10s", rs.getString("Classification")) + "\t" + rs.getString("MentorID")
							+ "\t" + rs.getDouble("GPA"));
				}
			} // End of Part C

			{ // Part D
				System.out.println("\nPart D\n");

				String query = "SELECT * FROM MeritList ORDER BY MentorID";
				rs = statement.executeQuery(query);
				HashMap<String, Integer> raise = new HashMap<>();
				while (rs.next()) {
					String m = rs.getString("MentorID");
					String c = rs.getString("Classification");
					int r = calculateRaise(c);
					if (raise.containsKey(m))
						raise.put(m, Math.max(raise.get(m), r));
					else
						raise.put(m, r);
				}

				query = "SELECT distinct i.InstructorID, i.Salary\n";
				query += "FROM Instructor i, MeritList m\n";
				query += "WHERE i.InstructorID = m.MentorID;";
				rs = statement.executeQuery(query);
				while (rs.next()) {
					String m = rs.getString("InstructorID");
					int s = rs.getInt("Salary");
					PreparedStatement ps = connection
							.prepareStatement("UPDATE Instructor SET Salary = ? WHERE InstructorID = ?");
					ps.setInt(1, (int) Math.round(s * (raise.get(m) + 100) / 100.0));
					ps.setString(2, m);
					ps.execute();
					System.out.println("update " + m + "\'s salary from " + String.format("%7s", s) + " to "
							+ String.format("%7s", (int) Math.round(s * (raise.get(m) + 100) / 100.0))
							+ String.format("%5s", raise.get(m)) + "%");
				}

			} // End of Part D

			{ // Part E
				System.out.println("\nPart E\n");

				String query = "";
				query += "SELECT\n";
				query += "p.Name, i.Salary\n";
				query += "FROM\n";
				query += "Person p,\n";
				query += "Instructor i\n";
				query += "WHERE\n";
				query += "p.ID = i.InstructorID;";

				rs = statement.executeQuery(query);

				int totalSalary = 0;
				int salary;

				while (rs.next()) {
					// get value of salary from each tuple
					salary = rs.getInt("Salary");
					System.out.println(rs.getString("Name") + "\t" + salary);
					totalSalary += salary;
				}
				String report = "Total Salary of all faculty: " + totalSalary;
				System.out.println(String.format("%" + report.length() + "s", "").replace(" ", "="));
				System.out.println(report);
			} // End of Part E

			{ // Part F
				System.out.println("\nPart F\n");

				String query = "DROP table MeritList";
				connection.prepareStatement(query).execute();

				System.out.println("MeritList has been dropped.\n");
			} // End of Part F

			// Close all statements and connections
			statement.close();
			rs.close();
			connection.close();

			System.out.println("*** Connection closed ***");

		} catch (SQLException e) {
			System.out.println("SQLException: " + e.getMessage());
			System.out.println("SQLState: " + e.getSQLState());
			System.out.println("VendorError: " + e.getErrorCode());
			e.printStackTrace();
		}
	}

	private static int calculateRaise(String classification) {
		int result = 0;
		if (classification.equals("Senior"))
			result = 10;
		else if (classification.equals("Junior"))
			result = 8;
		else if (classification.equals("Sophomore"))
			result = 6;
		else
			result = 4;
		return result;
	}

}
