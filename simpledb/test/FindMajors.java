package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import java.util.Scanner;

public class FindMajors {
   public static void main(String[] args) {
      System.out.print("Enter a department name: ");
      Scanner sc = new Scanner(System.in);
      String major = sc.next();
      sc.close();
      System.out.println("Here are the " + major + " majors");
      System.out.println("Name\tGradYear");

      String qry = "select sname, gradyear "
            + "from student, dept "
            + "where did = majorid "
            + "and dname = '" + major + "'";
 
      try {
         // analogous to the driver
         SimpleDB db = new SimpleDB("studentdb");

         // analogous to the connection
         Transaction tx  = db.newTx();
         Planner planner = db.planner();
         Plan p = planner.createQueryPlan(qry, tx);
         
         // analogous to the result set
         Scan rs = p.open();
         while (rs.next()) {
            String sname = rs.getString("sname");
            int gradyear = rs.getInt("gradyear");
            System.out.println(sname + "\t" + gradyear);
         }
         tx.commit();
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
