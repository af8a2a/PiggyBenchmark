import org.apache.commons.math3.random.RandomDataGenerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import static java.sql.Connection.TRANSACTION_SERIALIZABLE;

public class MySQLTest {
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/bank";

    // 数据库用户和密码
    static final String USER = "root";
    static final String PASS = "12345qqaa";
    static int WriteCount = 0;
    // 并发连接数
    static final int NUM_CONNECTIONS = 6;
    static final int NUM_TRANSACTIONS_PER_THREAD = 1000; // 每个线程的交易次数
    static final int MAX_RETRY = 8;

    public static void main(String[] args) {

        Thread[] threads = new Thread[NUM_CONNECTIONS];

        try {
            // 注册 JDBC 驱动程序
            Class.forName(JDBC_DRIVER);
            Connection setup_connection = DriverManager.getConnection(DB_URL, USER, PASS);

            setup_connection.setTransactionIsolation(TRANSACTION_SERIALIZABLE);
            long startTime = System.currentTimeMillis();
            setupDatabase(setup_connection);
            long endTime = System.currentTimeMillis();
            double elapsedTime = endTime - startTime;

            setupDatabase(setup_connection);
            System.out.println("Database setup completed successfully.");
            System.out.println("Database setup in " + elapsedTime + " milliseconds");
            setup_connection.close();

             startTime = System.currentTimeMillis();
            Connection connection = DriverManager.getConnection(DB_URL, USER, PASS);
            Random random = new Random();

            for (int i = 0; i < NUM_CONNECTIONS; i++) {

                threads[i] = new Thread(new TestRunnable(i));
                threads[i].start();
            }

            // 等待所有线程完成
            for (int i = 0; i < NUM_CONNECTIONS; i++) {
                threads[i].join();

            }
            double total = NUM_CONNECTIONS * NUM_TRANSACTIONS_PER_THREAD;
             endTime = System.currentTimeMillis();
             elapsedTime = endTime - startTime;
            System.out.println("Run "+total +" Transaction, Total time taken: " + elapsedTime + " milliseconds");
            System.out.println("Run " + total / (elapsedTime / 1000.0) + " Transaction Per sec");
            System.out.println("Run " + WriteCount + " Write Transaction");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setupDatabase(Connection connection) throws SQLException {

        try (Statement stmt = connection.createStatement()) {
            // 创建表
            String dropTableSQL = "DROP TABLE if EXISTS customer";
            stmt.addBatch(dropTableSQL);
            dropTableSQL="DROP TABLE if EXISTS account";
            stmt.addBatch(dropTableSQL);
            String createTableSQL = "CREATE TABLE customer (" +
                    "id INT PRIMARY KEY," +
                    "name VARCHAR(20) NOT NULL)";
            stmt.addBatch(createTableSQL);
            String createAccountSQL =
                    "CREATE TABLE account (" +
                            "id INT PRIMARY KEY," +
                            "customer_id INT," +
                            "balance INT NOT NULL)";
            stmt.addBatch(createAccountSQL);
            String createIndexSQL = "CREATE INDEX c_idx on account(customer_id)";
            stmt.addBatch(createIndexSQL);

            RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
            StringBuilder customerData = new StringBuilder();
            StringBuilder accountData = new StringBuilder();
            customerData.append("INSERT INTO customer VALUES ");
            accountData.append("INSERT INTO account VALUES ");
            for (int i = 1; i <= 1000; i++) {
                customerData.append(String.format("(%d,'%s')", i, randomDataGenerator.nextHexString(12)));
                accountData.append(String.format("(%d,%d,%d)", i, i, 100));
                if (i != 1000) {
                    customerData.append(",");
                    accountData.append(",");
                }
            }
            stmt.addBatch(customerData.toString());
            stmt.addBatch(accountData.toString());

            // 插入初始数据
            try {
                stmt.executeBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }


        }
    }

    static class TestRunnable implements Runnable {
        private int threadId;

        TestRunnable(int threadId) {
            this.threadId = threadId;
        }

        @Override
        public void run() {
            try {
                // 这里可以执行数据库操作
                // 例如执行查询或更新
                // 使用 connection 对象执行数据库操作
                System.out.println("Thread " + threadId + " is executing...");

                Connection connection = DriverManager.getConnection(DB_URL, USER, PASS);
                connection.setTransactionIsolation(TRANSACTION_SERIALIZABLE);
                Random random = new Random();
                for (int i = 0; i < NUM_TRANSACTIONS_PER_THREAD; i++) {
                    int accountFrom = random.nextInt(1000) + 1; // 1-1000
                    int accountTo = random.nextInt(1000) + 1; // 1-1000
                    int amount = random.nextInt(1000) + 1; // 1-1000
                    transferMoney(connection, accountFrom, accountTo, amount);


                }
                connection.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private static void transferMoney(Connection connection, int accountFrom, int accountTo, int amount) throws SQLException {
            boolean success = false;
            int retryCount = 0;

            while (!success && retryCount < MAX_RETRY) {
                try {
                    Statement stmt = connection.createStatement();

                    String sql = String.format("SELECT a.id, a.balance" +
                            " FROM account a  JOIN customer c ON a.customer_id = c.id" +
                            " WHERE c.id = %d" +
                            " ORDER BY a.balance DESC" +
                            " LIMIT 1;", accountFrom);


                    stmt.executeQuery(sql);

                    sql = String.format(
                            "SELECT a.id, a.balance " +
                                    "FROM account a JOIN customer c ON a.customer_id = c.id " +
                                    "WHERE c.id = %d " +
                                    "ORDER BY a.balance ASC " +
                                    "LIMIT 1;",
                            accountTo
                    );
                    stmt.executeQuery(sql);
                    Random random = new Random();
                    int write_rate = random.nextInt(100) + 1;

                    if (write_rate > 0) {
                        WriteCount++;
                        sql = String.format("UPDATE account SET balance = balance - %d WHERE id = %d;", amount, accountFrom);
                        stmt.addBatch(sql);
                        sql = String.format("UPDATE account SET balance = balance + %d WHERE id = %d;", amount, accountTo);
                        stmt.addBatch(sql);
                    }
                    stmt.executeBatch();
                    success = true;
                } catch (SQLException e) {
//                    e.printStackTrace();
                    retryCount++;
                    try {
                        Random random = new Random();
                        int wait_fator = random.nextInt(10) + 1;
                        Thread.sleep((long) (wait_fator * Math.pow(2, retryCount - 1)));
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }


                }
            }
        }

    }

}
