package simpledb.test;

import simpledb.tx.Transaction;
// import simpledb.plan.Plan;
import simpledb.plan.Planner;
// import simpledb.query.*;
import simpledb.server.SimpleDB;

public class ChangeMajor {
   public static void main(String[] args) {

      try {
         // analogous to the driver
         SimpleDB db = new SimpleDB("studentdb");

         // analogous to the connection
         Transaction tx  = db.newTx();
         Planner planner = db.planner();

         String cmd = "update STUDENT "
                    + "set MajorId=30 "
                    + "where SName = 'amy'";
         planner.executeUpdate(cmd, tx);
         System.out.println("Amy is now a drama major.");
         
         tx.commit();
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
