import java.sql.*;
import java.util.Random;

public class SITest {
    // JDBC 驱动程序和数据库 URL
    static final String JDBC_DRIVER = "org.postgresql.Driver";
    //for postgresql "jdbc:postgresql://localhost:5432/your_database";
    static final String DB_URL = "jdbc:postgresql://localhost:5432/your_database";

    // 数据库用户和密码
    static final String USER = "root";
    static final String PASS = "12345qqaa";
    static int WriteCount = 0;
    static int RollbackCount = 0;
    // 并发连接数
    static final int NUM_CONNECTIONS = 3;
    static final int NUM_TRANSACTIONS_PER_THREAD = 1000; // 每个线程的交易次数
    static final int MAX_RETRY = 16;

    public static void main(String[] args) {
        Connection setup_connection = null;

        Thread[] threads = new Thread[NUM_CONNECTIONS];

        try {
            // 注册 JDBC 驱动程序
            Class.forName(JDBC_DRIVER);
            setup_connection = DriverManager.getConnection(DB_URL, USER, PASS);
            long startTime = System.currentTimeMillis();
            setupDatabase(setup_connection);
            long endTime = System.currentTimeMillis();
            double elapsedTime = endTime - startTime;
            System.out.println("Database setup completed successfully.");
            System.out.println("Database setup in " + elapsedTime + " milliseconds");
            setup_connection.close();

             startTime = System.currentTimeMillis();

            // 创建连接并发测试
            for (int i = 0; i < NUM_CONNECTIONS; i++) {

                threads[i] = new Thread(new SSIThread(i));
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
            System.out.println("Rollback " + RollbackCount + " Transaction");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setupDatabase(Connection connection) throws SQLException {

        try (Statement stmt = connection.createStatement()) {
            String dropTableSQL = "DROP TABLE if EXISTS ssi_test";
            stmt.addBatch(dropTableSQL);

            // 创建表
            String createTableSQL = "CREATE TABLE  ssi_test (id INT PRIMARY KEY) ";
            stmt.addBatch(createTableSQL);

            StringBuilder ssiData = new StringBuilder();
            ssiData.append("INSERT INTO ssi_test VALUES (1);");
            stmt.addBatch(ssiData.toString());

            // 插入初始数据
            try {
                stmt.executeBatch();
                stmt.executeQuery("select id from ssi_test");
                ResultSet result= stmt.getResultSet();
                while(result.next()){
                    System.out.println("result = "+result.getInt("id"));
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }


        }
    }

    static class SSIThread implements Runnable {
        private int threadId;

        SSIThread(int threadId) {
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

                Random random = new Random();
                for (int i = 0; i < NUM_TRANSACTIONS_PER_THREAD; i++) {

                    SSIUpdate(connection);
                }


            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private static void SSIUpdate(Connection connection) throws SQLException {
            boolean success = false;
            int retryCount = 0;

            while (!success && retryCount < MAX_RETRY) {
                try {
                    Statement stmt = connection.createStatement();
                    stmt.executeUpdate("UPDATE ssi_test set id=id+1");
                    success = true;
                } catch (SQLException e) {
//                    e.printStackTrace();
                    RollbackCount++;
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
