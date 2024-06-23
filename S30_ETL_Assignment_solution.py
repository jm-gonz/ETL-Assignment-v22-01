import sqlite3
import pandas as pd
import csv

class EtlAssignment:

    db_connection = None
    db_cursor = None
    db_tables = ['items', 'customers', 'sales', 'orders']
    df_items = None
    df_customers = None
    df_sales = None
    df_orders = None
    

    def __init__(self) -> None:
        self.db_connection = self.connect_db()
        self.db_cursor = self.db_connection.cursor()
        self.get_dataframes()
        self.merge_dataframes()
        self.clean_up_dataframes()

    def connect_db(self):
        db_connection = sqlite3.connect('S30 ETL Assignment.db')
        return db_connection
    
    def get_dataframes(self):
        for table in self.db_tables:
            setattr(self, f'df_{table}', self.table_to_dataframe(table))

    def table_to_dataframe(self, table):
        query = f"SELECT * FROM {table};"
        df = pd.read_sql_query(query, self.db_connection)
        return df
    
    def close_connection(self):
        if self.db_connection:
            self.db_connection.close()
        if self.db_cursor:
            self.db_cursor
    
    def merge_dataframes(self):
        self.df_sales = self.df_sales.merge(self.df_customers, how = "left", on = "customer_id")
        self.df_orders = self.df_orders.merge(self.df_items, how = "left", on = "item_id")
        self.df_orders = self.df_orders.merge(self.df_sales, how = "left", on = "sales_id")

    def clean_up_dataframes(self):
        drop_columns = ["item_id", "sales_id", "order_id"] 

        self.df_orders["quantity"] = self.df_orders["quantity"].fillna(0)

        self.df_orders["quantity"] = self.df_orders["quantity"].astype(int)
        self.df_orders["age"] = self.df_orders["age"].astype(int)

        self.df_orders = self.df_orders.drop(drop_columns, axis = 1)

    
    def pandas_filter_solution(self): 
        mask = self.df_orders["age"] >= 18 
        mask &= self.df_orders["age"] <= 35
        df_result = self.df_orders[mask]

        group_by_columns = ["item_name", "customer_id", "age"]
        df_result =df_result.groupby(group_by_columns)["quantity"].sum().reset_index()

        column_format = ["customer_id", "age", "item_name", "quantity"]
        df_result = df_result[column_format]
        df_result = df_result.sort_values(column_format, ascending=True) 

        print(df_result)
        df_result.to_csv('pandas_solution.csv', sep=';', index=False)

    def sql_filter_solution(self):
        query = """
        SELECT 
            customers.customer_id,
            customers.age,
            items.item_name,
            SUM(COALESCE(orders.quantity, 0)) AS total_quantity
        FROM 
            orders
        LEFT JOIN 
            items ON items.item_id = orders.item_id
        LEFT JOIN 
            sales ON sales.sales_id = orders.sales_id
        LEFT JOIN 
            customers ON customers.customer_id = sales.customer_id
        WHERE 
            customers.age >= 18 AND customers.age <= 35
        GROUP BY 
            items.item_name, 
            customers.customer_id, 
            customers.age
        ORDER BY 
            customers.customer_id ASC;
        """
        self.db_cursor.execute(query)
        rows = self.db_cursor.fetchall()

        column_format = ["customer_id", "age", "item_name", "quantity"]
        for row in rows:
            row_dict = dict(zip(column_format, row))
            print(row_dict)
        
        self.convert_sql_results_to_csv(rows, column_format)

    def convert_sql_results_to_csv(self, rows, column_format):
        csv_file = 'sql_solution.csv'
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=';') 
            writer.writerow(column_format)
            writer.writerows(rows)

if __name__ == "__main__":

    etl_assignment = EtlAssignment()
    print("PANDAS SOLUTION: ")
    etl_assignment.pandas_filter_solution()

    print("SQL SOLUTION: ")
    etl_assignment.sql_filter_solution()

    etl_assignment.close_connection()
