package org.apache.kafka.connect.transforms.util;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class TargetColumnInfo {
//    private static String sql = "SELECT COL_NM, DMN_PNM, ENCRP_CD, ENCRP_KEY FROM TB_ENCRP_KEY_INFO";


    public static Map<String, String[]> getColumnInfo(String ip, String port, String schema, String user, String pwd, String driver, String insName){
        String sql = "SELECT COL_NM, DMN_PNM, ENCRP_CD, ENCRP_KEY FROM "+insName+".TB_ENCRP_KEY_INFO";
        System.out.println(":LINASTDOUT: SQL String :"+sql);
        String url  = "jdbc:oracle:thin:@"+ip+":"+port+":"+schema;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, pwd);
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();

            Map<String, String[]> infoHmap = new HashMap<>();
            System.out.println("-------------------------------------------------------");
            while (rs.next()) {
                String[] info = new String[3];
                String key = rs.getString("COL_NM");
                info[0] = rs.getString("DMN_PNM");
                info[1] = rs.getString("ENCRP_CD");
                info[2] = rs.getString("ENCRP_KEY");

                infoHmap.put(key, info);
                System.out.printf(":LINASTDOUT: %s %s %s %s%n", key, info[0], info[1], info[2]);
            }
//            System.out.println(infoHmap.containsKey("CNSL_NO"));
//
            infoHmap.forEach((key, value)->{
                System.out.println("col_nm:"+key);
                System.out.println("dmn_pnm:"+value[0]);
                System.out.println("encrp_cd:"+value[1]);
                System.out.println("encrp_key:"+value[2]);
            });
            return infoHmap;

        }catch (SQLException sqle) {
            System.out.println("[:LINAERROR:SQL Error : " + sqle.getMessage() + "]");
        } catch (ClassNotFoundException cnfe) {
            System.out.println("[:LINAERROR:JDBC Connector Driver ClassNotFoundException : " + cnfe.getMessage() + "]");
        }catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}
