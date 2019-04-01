/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tpch.queryexecutor;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author nihar_kapadia
 */
public class TPCHQueryExecutor {

    public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) throws ClassNotFoundException, SQLException, ParseException {

        try {
            System.out.println("=========== TPC-H Query Executor ====================");
            int number = 0;
            try {
                number = Integer.parseInt(args[0]);
                if (number > 22 || number < 1) {
                    System.out.println("Please enter valid query number from 1 to 22");
                    return;
                }
            } catch (Exception e) {
                System.out.println("Please enter valid query number from 1 to 22");
                return;
            }

            Class.forName("com.gigaspaces.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:insightedge:url=jini://localhost/*/space;logLevel=debug");
            //Connection connection = DriverManager.getConnection("jdbc:insightedge:url=jini://3.95.232.250:4174/*/space;");

            String methodName = "runQuery" + number;
            System.out.println("methodName== "+methodName);
            Method method = TPCHQueryExecutor.class.getMethod(methodName, Connection.class);
            Object o = method.invoke(null, connection);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
    public static void runQuery1(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-1");

            String sql = "select LReturnflag, LLinestatus, sum(LQuantity) as sum_qty, sum(LExtendedprice) as sum_base_price"
                    + ", sum(LExtendedprice * (1 - LDiscount)) as sum_disc_price"
                    + ", sum(LExtendedprice * (1 - LDiscount) * (1 + LTax)) as sum_charge"
                    + ", avg(LQuantity) as avg_qty, avg(LExtendedprice) as avg_price"
                    + ", avg(LDiscount) as avg_disc, count(*) as count_order "
                    + " from Lineitem where "
                    + " LShipdate <= ? group by LReturnflag, LLinestatus order by LReturnflag, LLinestatus";

            // '1998-12-01' - interval '90' day(3) ==  "1998-09-02"
            java.util.Date date = formatter.parse("1998-09-02");

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new java.sql.Date(date.getTime()));
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));

            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (ParseException ex) {
            ex.printStackTrace();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void runQuery2(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-2");
            
            String sql = "select SAcctbal, SName, NName, PPartkey, PMfgr, SAddress, SPhone, SComment "
                    + " from Part, Supplier, Partsupp, Nation, Region "
                    + " where PPartkey = psPartkey "
                    + " and SSuppkey = psSuppkey "
                    + " and PSize = 30 "
                    + " and PType like '%BRASS' "
                    + " and SNationkey = NNationkey "
                    + " and NRegionkey = RRegionkey "
                    + " and RName = 'EUROPE' "
                    + " and psSupplycost = "
                    + " (select min(psSupplycost) "
                    + " from Partsupp, Supplier, Nation, Region "
                    + " where PPartkey = psPartkey "
                    + " and SSuppkey = psSuppkey "
                    + " and SNationkey = NNationkey "
                    + " and NRegionkey = RRegionkey "
                    + " and RName = 'EUROPE') "
                    + " order by SAcctbal desc, NName, SName, PPartkey limit 100";

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery3(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-3");

            String sql = "SELECT LOrderkey, "
                    + "       SUM(LExtendedprice * ( 1 - LDiscount )) AS revenue, "
                    + "       OOrderdate, "
                    + "       OShippriority "
                    + "FROM Customer, "
                    + "       Orders, "
                    + "       Lineitem "
                    + "WHERE CMktsegment = 'BUILDING' "
                    + "       AND CCustkey = OCustkey "
                    + "       AND LOrderkey = OOrderkey "
                    + "       AND OOrderdate < ? "
                    + "       AND LShipdate > ? "
                    + "GROUP BY LOrderkey, "
                    + "          OOrderdate, "
                    + "          OShippriority "
                    + "ORDER BY revenue DESC,OOrderdate";

            java.util.Date date = formatter.parse("1995-03-15");

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new java.sql.Date(date.getTime()));
            preparedStatement.setDate(2, new java.sql.Date(date.getTime()));
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery4(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-4");

            String sql = "SELECT OOrderpriority, "
                    + "Count(*) order_count "
                    + "FROM Orders "
                    + "WHERE OOrderdate >= ? "
                    + "AND OOrderdate < ? "
                    + "AND EXISTS (SELECT * "
                    + "FROM Lineitem "
                    + "WHERE LOrderkey = OOrderkey "
                    + "AND LCommitdate < LReceiptdate) "
                    + "GROUP BY OOrderpriority "
                    + "ORDER BY OOrderpriority";

            //DATE '1993-07-01' + interval '3' month  == 1993-10-01
            java.util.Date date1 = formatter.parse("1993-07-01");
            java.util.Date date2 = formatter.parse("1993-10-01");

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new java.sql.Date(date1.getTime()));
            preparedStatement.setDate(2, new java.sql.Date(date2.getTime()));
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            ex.printStackTrace();
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
    }

    public static void runQuery5(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-5");

            String sql = "SELECT NName, "
                    + "       SUM(LExtendedprice * ( 1 - LDiscount )) AS revenue "
                    + "FROM   Customer, "
                    + "       Orders, "
                    + "       Lineitem, "
                    + "       Supplier, "
                    + "       Nation, "
                    + "       Region "
                    + "WHERE  CCustkey = OCustkey "
                    + "       AND LOrderkey = OOrderkey "
                    + "       AND LSuppkey = SSuppkey "
                    + "       AND CNationkey = SNationkey "
                    + "       AND SNationkey = NNationkey "
                    + "       AND NRegionkey = RRegionkey "
                    + "       AND RName = 'ASIA' "
                    + "       AND OOrderdate >= ? "
                    + "       AND OOrderdate < ? "
                    + "GROUP  BY NName "
                    + "ORDER  BY revenue DESC";

            // DATE '1994-01-01' + interval '1' year == 1995-01-01
            java.util.Date date1 = formatter.parse("1994-01-01");
            java.util.Date date2 = formatter.parse("1995-01-01");

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new java.sql.Date(date1.getTime()));
            preparedStatement.setDate(2, new java.sql.Date(date2.getTime()));
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            ex.printStackTrace();
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
    }

    public static void runQuery6(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-6");

            String sql = "SELECT SUM(LExtendedprice * LDiscount) AS revenue "
                    + "FROM Lineitem "
                    + "WHERE LShipdate >= ? "
                    + "AND LShipdate < ? "
                    + "AND LDiscount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01 "
                    + "AND LQuantity < 24";

            // DATE '1994-01-01' + interval '1' year  == 1995-01-01
            java.util.Date date1 = formatter.parse("1994-01-01");
            java.util.Date date2 = formatter.parse("1995-01-01");

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new java.sql.Date(date1.getTime()));
            preparedStatement.setDate(2, new java.sql.Date(date2.getTime()));
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery7(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-7");

            String sql = "SELECT supp_nation, "
                    + "cust_nation, "
                    + "l_year, "
                    + "SUM(volume) AS revenue "
                    + "FROM (SELECT n1.NName AS supp_nation, "
                    + "n2.NName AS cust_nation, "
                    + "Extract(year FROM LShipdate) AS l_year, "
                    + "LExtendedprice * ( 1 - LDiscount ) AS volume "
                    + "FROM   Supplier, "
                    + "Lineitem, "
                    + "Orders, "
                    + "Customer, "
                    + "Nation n1, "
                    + "Nation n2 "
                    + "WHERE  SSuppkey = LSuppkey "
                    + "AND OOrderkey = LOrderkey "
                    + "AND CCustkey = OCustkey "
                    + "AND SNationkey = n1.NNationkey "
                    + "AND CNationkey = n2.NNationkey "
                    + "AND ( ( n1.NName = 'FRANCE' "
                    + "AND n2.NName = 'GERMANY' ) "
                    + "OR ( n1.NName = 'GERMANY' "
                    + "AND n2.NName = 'FRANCE' ) ) "
                    + "AND LShipdate BETWEEN ? AND ?) "
                    + "AS shipping "
                    + "GROUP  BY supp_nation, "
                    + "cust_nation, "
                    + "l_year "
                    + "ORDER  BY supp_nation, "
                    + "          cust_nation, "
                    + "          l_year";

            java.util.Date date1 = formatter.parse("1995-01-01");
            java.util.Date date2 = formatter.parse("1996-12-31");

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new java.sql.Date(date1.getTime()));
            preparedStatement.setDate(2, new java.sql.Date(date2.getTime()));
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery8(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-8");

            String sql = "SELECT o_year, "
                    + "       SUM(CASE "
                    + "             WHEN nation = 'BRAZIL' THEN volume "
                    + "             ELSE 0 "
                    + "           END) / SUM(volume) AS mkt_share "
                    + "FROM   (SELECT Extract(year FROM OOrderdate)       AS o_year, "
                    + "               LExtendedprice * ( 1 - LDiscount ) AS volume, "
                    + "               n2.NName                            AS nation "
                    + "        FROM   Part, "
                    + "               Supplier, "
                    + "               Lineitem, "
                    + "               Orders, "
                    + "               Customer, "
                    + "               Nation n1, "
                    + "               Nation n2, "
                    + "               Region "
                    + "        WHERE  PPartkey = LPartkey "
                    + "               AND SSuppkey = LSuppkey "
                    + "               AND LOrderkey = OOrderkey "
                    + "               AND OCustkey = CCustkey "
                    + "               AND CNationkey = n1.NNationkey "
                    + "               AND n1.NRegionkey = RRegionkey "
                    + "               AND RName = 'AMERICA' "
                    + "               AND SNationkey = n2.NNationkey "
                    + "               AND OOrderdate BETWEEN ? AND ? "
                    + "               AND PType = 'ECONOMY ANODIZED STEEL') AS all_nations "
                    + "GROUP  BY o_year "
                    + "ORDER  BY o_year";

            java.util.Date date1 = formatter.parse("1995-01-01");
            java.util.Date date2 = formatter.parse("1996-12-31");

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new java.sql.Date(date1.getTime()));
            preparedStatement.setDate(2, new java.sql.Date(date2.getTime()));
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery9(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-9");

            String sql = "SELECT nation, "
                    + "       o_year, "
                    + "       Sum(amount) AS sum_profit "
                    + "FROM   (SELECT NName "
                    + "               AS "
                    + "                      nation, "
                    + "               Extract(year FROM OOrderdate) "
                    + "               AS "
                    + "                      o_year, "
                    + "               LExtendedprice * ( 1 - LDiscount ) - psSupplycost * LQuantity "
                    + "               AS "
                    + "                      amount "
                    + "        FROM   Part, "
                    + "               Supplier, "
                    + "               Lineitem, "
                    + "               Partsupp, "
                    + "               Orders, "
                    + "               Nation "
                    + "        WHERE  SSuppkey = LSuppkey "
                    + "               AND psSuppkey = LSuppkey "
                    + "               AND psPartkey = LPartkey "
                    + "               AND PPartkey = LPartkey "
                    + "               AND OOrderkey = LOrderkey "
                    + "               AND SNationkey = NNationkey "
                    + "               AND PName LIKE '%green%') AS profit "
                    + "GROUP  BY nation, "
                    + "          o_year "
                    + "ORDER  BY nation, "
                    + "          o_year DESC";

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery10(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-10");

            String sql = "SELECT CCustkey, "
                    + "       CName, "
                    + "       SUM(LExtendedprice * ( 1 - LDiscount )) AS revenue, "
                    + "       CAcctbal, "
                    + "       NName, "
                    + "       CAddress, "
                    + "       CPhone, "
                    + "       CComment "
                    + "FROM   Customer, "
                    + "       Orders, "
                    + "       Lineitem, "
                    + "       Nation "
                    + "WHERE  CCustkey = OCustkey "
                    + "       AND LOrderkey = OOrderkey "
                    + "       AND OOrderdate >= ? "
                    + "       AND OOrderdate < ? "
                    + "       AND LReturnflag = 'R' "
                    + "       AND CNationkey = NNationkey "
                    + "GROUP  BY CCustkey, "
                    + "          CName, "
                    + "          CAcctbal, "
                    + "          CPhone, "
                    + "          NName, "
                    + "          CAddress, "
                    + "          CComment "
                    + "ORDER  BY revenue DESC";

            //DATE '1993-10-01' + interval '3' month == 1994-01-01
            java.util.Date date1 = formatter.parse("1993-10-01");
            java.util.Date date2 = formatter.parse("1994-01-01");

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new java.sql.Date(date1.getTime()));
            preparedStatement.setDate(2, new java.sql.Date(date2.getTime()));
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

        public static void runQuery11(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-11");

            String sql = "SELECT psPartkey, "
                    + "Sum(psSupplycost * psAvailqty) AS value1 "
                    + "FROM   Partsupp, "
                    + "Supplier, "
                    + "Nation "
                    + "WHERE  psSuppkey = SSuppkey "
                    + "AND SNationkey = NNationkey "
                    + "AND NName = 'GERMANY' "
                    + "GROUP  BY psPartkey "
                    + "HAVING Sum(psSupplycost * psAvailqty) > (SELECT "
                    + "Sum(psSupplycost * psAvailqty) * 0.0001000000 "
                    + "FROM   Partsupp, "
                    + "Supplier, "
                    + "Nation "
                    + "WHERE  psSuppkey = SSuppkey "
                    + "AND SNationkey = NNationkey "
                    + "AND NName = 'GERMANY') "
                    + "ORDER  BY value1 DESC";

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery12(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-12");

            String sql = "SELECT LShipmode, "
                    + "       SUM(CASE "
                    + "             WHEN OOrderpriority = '1-URGENT' "
                    + "                   OR OOrderpriority = '2-HIGH' THEN 1 "
                    + "             ELSE 0 "
                    + "           END) AS high_line_count, "
                    + "       SUM(CASE "
                    + "             WHEN OOrderpriority <> '1-URGENT' "
                    + "                  AND OOrderpriority <> '2-HIGH' THEN 1 "
                    + "             ELSE 0 "
                    + "           END) AS low_line_count "
                    + "FROM   Orders, "
                    + "       Lineitem "
                    + "WHERE  OOrderkey = LOrderkey "
                    + "       AND LShipmode IN ( 'MAIL', 'SHIP' ) "
                    + "       AND LCommitdate < LReceiptdate "
                    + "       AND LShipdate < LCommitdate "
                    + "       AND LReceiptdate >= ? "
                    + "       AND LReceiptdate < ? "
                    + "GROUP  BY LShipmode "
                    + "ORDER  BY LShipmode";

            //DATE '1994-01-01' + interval '1' year == 
            java.util.Date date1 = formatter.parse("1994-01-01");
            java.util.Date date2 = formatter.parse("1995-01-01");

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new java.sql.Date(date1.getTime()));
            preparedStatement.setDate(2, new java.sql.Date(date2.getTime()));
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery13(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-13");

            String sql = "SELECT CCount, "
                    + "       Count(*) AS custdist "
                    + "FROM   (SELECT CCustkey, "
                    + "               Count(o_orderkey) "
                    + "        FROM   Customer "
                    + "               LEFT OUTER JOIN Orders "
                    + "                            ON CCustkey = OCustkey "
                    + "                               AND OComment NOT LIKE '%special%requests%' "
                    + "        GROUP  BY CCustkey) AS c_orders (CCustkey, CCount) "
                    + "GROUP  BY CCount "
                    + "ORDER  BY custdist DESC, "
                    + "          CCount DESC";

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery14(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-14");

            String sql = "SELECT 100.00 * SUM(CASE "
                    + "                      WHEN PType LIKE 'PROMO%' THEN LExtendedprice * "
                    + "                                                     ( 1 - LDiscount ) "
                    + "                      ELSE 0 "
                    + "                    END) / SUM(LExtendedprice * ( 1 - LDiscount )) AS "
                    + "       promo_revenue "
                    + "FROM   Lineitem, "
                    + "       Part "
                    + "WHERE  LPartkey = PPartkey "
                    + "       AND LShipdate >= ? "
                    + "       AND LShipdate < ? ";

            //DATE '1995-09-01' + interval '1' month  ==1995-10-01
            java.util.Date date1 = formatter.parse("1995-09-01");
            java.util.Date date2 = formatter.parse("1995-10-01");

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new java.sql.Date(date1.getTime()));
            preparedStatement.setDate(2, new java.sql.Date(date2.getTime()));
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery15(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-15");

            String sql = "CREATE VIEW revenue0 "
                    + "(supplier_no, total_revenue) "
                    + "AS "
                    + "  SELECT LSuppkey, "
                    + "         SUM(LExtendedprice * ( 1 - LDiscount )) "
                    + "  FROM   Lineitem "
                    + "  WHERE  LShipdate >= ? "
                    + "         AND LShipdate < ? "
                    + "  GROUP  BY LSuppkey; "
                    + ""
                    + "SELECT SSuppkey, "
                    + "       SName, "
                    + "       SAddress, "
                    + "       SPhone, "
                    + "       total_revenue "
                    + "FROM   Supplier, "
                    + "       revenue0 "
                    + "WHERE  SSuppkey = supplier_no "
                    + "       AND total_revenue = (SELECT Max(total_revenue) "
                    + "                            FROM   revenue0) "
                    + "ORDER  BY SSuppkey";

            // DATE '1996-01-01' + interval '3' month == 1996-04-01
            java.util.Date date1 = formatter.parse("1995-03-15");
            java.util.Date date2 = formatter.parse("1996-04-01");

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new java.sql.Date(date1.getTime()));
            preparedStatement.setDate(2, new java.sql.Date(date2.getTime()));
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery16(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-16");

            String sql = "SELECT PBrand, "
                    + "       PType, "
                    + "       PSize, "
                    + "       Count(DISTINCT psSuppkey) AS supplier_cnt "
                    + "FROM   Partsupp, "
                    + "       Part "
                    + "WHERE  PPartkey = psPartkey "
                    + "       AND PBrand <> 'Brand#45' "
                    + "       AND PType NOT LIKE 'MEDIUM POLISHED%' "
                    + "       AND PSize IN ( 49, 14, 23, 45, "
                    + "                       19, 3, 36, 9 ) "
                    + "       AND psSuppkey NOT IN (SELECT SSuppkey "
                    + "                              FROM   Supplier "
                    + "                              WHERE  SComment LIKE '%Customer%Complaints%') "
                    + "GROUP  BY PBrand, "
                    + "          PType, "
                    + "          PSize "
                    + "ORDER  BY supplier_cnt DESC, "
                    + "          PBrand, "
                    + "          PType, "
                    + "          PSize";

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery17(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-17");

            String sql = "SELECT Sum(LExtendedprice) / 7.0 AS avg_yearly "
                    + "FROM   Lineitem, "
                    + "       Part "
                    + "WHERE  PPartkey = LPartkey "
                    + "       AND PBrand = 'Brand#23' "
                    + "       AND PContainer = 'MED BOX' "
                    + "       AND LQuantity < (SELECT 0.2 * Avg(LQuantity) "
                    + "                         FROM   Lineitem "
                    + "                         WHERE  LPartkey = PPartkey)";

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery18(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-18");

            String sql = "SELECT CName, "
                    + "       CCustkey, "
                    + "       OOrderkey, "
                    + "       OOrderdate, "
                    + "       OTotalprice, "
                    + "       Sum(LQuantity) "
                    + "FROM   Customer, "
                    + "       Orders, "
                    + "       Lineitem "
                    + "WHERE  OOrderkey IN (SELECT LOrderkey "
                    + "                      FROM   Lineitem "
                    + "                      GROUP  BY LOrderkey "
                    + "                      HAVING Sum(LQuantity) > 300) "
                    + "       AND CCustkey = OCustkey "
                    + "       AND OOrderkey = LOrderkey "
                    + "GROUP  BY CName, "
                    + "          CCustkey, "
                    + "          OOrderkey, "
                    + "          OOrderdate, "
                    + "          OTotalprice "
                    + "ORDER  BY OTotalprice DESC, "
                    + "          OOrderdate";

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery19(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-19");

            String sql = "SELECT Sum(LExtendedprice * ( 1 - LDiscount )) AS revenue "
                    + "FROM   Lineitem, "
                    + "       Part "
                    + "WHERE  ( PPartkey = LPartkey "
                    + "         AND PBrand = 'Brand#12' "
                    + "         AND PContainer IN ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG' ) "
                    + "         AND LQuantity >= 1 "
                    + "         AND LQuantity <= 1 + 10 "
                    + "         AND PSize BETWEEN 1 AND 5 "
                    + "         AND LShipmode IN ( 'AIR', 'AIR REG' ) "
                    + "         AND LShipinstruct = 'DELIVER IN PERSON' ) "
                    + "        OR ( PPartkey = LPartkey "
                    + "             AND PBrand = 'Brand#23' "
                    + "             AND PContainer IN ( 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK' ) "
                    + "             AND LQuantity >= 10 "
                    + "             AND LQuantity <= 10 + 10 "
                    + "             AND PSize BETWEEN 1 AND 10 "
                    + "             AND LShipmode IN ( 'AIR', 'AIR REG' ) "
                    + "             AND LShipinstruct = 'DELIVER IN PERSON' ) "
                    + "        OR ( PPartkey = LPartkey "
                    + "             AND PBrand = 'Brand#34' "
                    + "             AND PContainer IN ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG' ) "
                    + "             AND LQuantity >= 20 "
                    + "             AND LQuantity <= 20 + 10 "
                    + "             AND PSize BETWEEN 1 AND 15 "
                    + "             AND LShipmode IN ( 'AIR', 'AIR REG' ) "
                    + "             AND LShipinstruct = 'DELIVER IN PERSON' ) ";

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery20(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-20");

            String sql = "SELECT SName, "
                    + "       SAddress "
                    + "FROM   Supplier, "
                    + "       Nation "
                    + "WHERE  SSuppkey IN (SELECT psSuppkey "
                    + "                     FROM   Partsupp "
                    + "                     WHERE  psPartkey IN (SELECT PPartkey "
                    + "                                           FROM   Part "
                    + "                                           WHERE  PName LIKE 'forest%') "
                    + "                            AND psAvailqty > (SELECT 0.5 * SUM(LQuantity) "
                    + "                                               FROM   Lineitem "
                    + "                                               WHERE  LPartkey = psPartkey "
                    + "                                                      AND LSuppkey = psSuppkey "
                    + "                                                      AND LShipdate >= ? "
                    + "                                                      AND LShipdate < ? "
                    + "                                              )) "
                    + "       AND SNationkey = NNationkey "
                    + "       AND NName = 'CANADA' "
                    + "ORDER  BY SName";

            java.util.Date date1 = formatter.parse("1994-01-01");
            java.util.Date date2 = formatter.parse("1995-01-01");

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new java.sql.Date(date1.getTime()));
            preparedStatement.setDate(2, new java.sql.Date(date2.getTime()));
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery21(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-21");

            String sql = "SELECT SName, "
                    + "       Count(*) AS numwait "
                    + "FROM   Supplier, "
                    + "       Lineitem l1, "
                    + "       Orders, "
                    + "       Nation "
                    + "WHERE  SSuppkey = l1.LSuppkey "
                    + "       AND OOrderkey = l1.LOrderkey "
                    + "       AND OOrderstatus = 'F' "
                    + "       AND l1.LReceiptdate > l1.LCommitdate "
                    + "       AND EXISTS (SELECT * "
                    + "                   FROM   Lineitem l2 "
                    + "                   WHERE  l2.LOrderkey = l1.LOrderkey "
                    + "                          AND l2.LSuppkey <> l1.LSuppkey) "
                    + "       AND NOT EXISTS (SELECT * "
                    + "                       FROM   Lineitem l3 "
                    + "                       WHERE  l3.LOrderkey = l1.LOrderkey "
                    + "                              AND l3.LSuppkey <> l1.LSuppkey "
                    + "                              AND l3.LReceiptdate > l3.LCommitdate) "
                    + "       AND SNationkey = NNationkey "
                    + "       AND NName = 'SAUDI ARABIA' "
                    + "GROUP  BY SName "
                    + "ORDER  BY numwait DESC, "
                    + "          SName";

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void runQuery22(Connection connection) {
        try {
            System.out.println("");
            System.out.println("Start TPCH Query-22");

            String sql = "SELECT cntrycode, "
                    + "       Count(*)       AS numcust, "
                    + "       Sum(CAcctbal) AS totacctbal "
                    + "FROM   (SELECT Substring(CPhone FROM 1 FOR 2) AS cntrycode, "
                    + "               CAcctbal "
                    + "        FROM   Customer "
                    + "        WHERE  Substring(CPhone FROM 1 FOR 2) IN ( '13', '31', '23', '29', "
                    + "                                                    '30', '18', '17' ) "
                    + "               AND CAcctbal > (SELECT Avg(CAcctbal) "
                    + "                                FROM   Customer "
                    + "                                WHERE  CAcctbal > 0.00 "
                    + "                                       AND Substring(CPhone FROM 1 FOR 2) IN ( "
                    + "                                           '13', '31', '23', '29', "
                    + "                                           '30', '18', '17' )) "
                    + "               AND NOT EXISTS (SELECT * "
                    + "                               FROM   Orders "
                    + "                               WHERE  OCustkey = CCustkey)) AS custsale "
                    + "GROUP  BY cntrycode "
                    + "ORDER  BY cntrycode";

            long start = System.currentTimeMillis();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            System.out.println("Total time in millis = " + (System.currentTimeMillis() - start));
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            System.out.println("Total records = " + count);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
