package com.assignment.rds.bdm;

import java.sql.Connection;

public class SQLonRDS {



    public static void main(String[] args) {

    }


    abstract class Coffee {
        Coffee() {
            System.out.println("Inside constructor coffee");
        }
    }

    class ColdCoffee extends Coffee {
        ColdCoffee() {
            System.out.println("Coldcoffee");
        }
    }

    public class AbstractClassTesting {
        public static void main(String[] args) {
            ColdCoffee cf = new ColdCoffee();
        }
    }

}
