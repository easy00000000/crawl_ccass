import MySQLdb

def split_table():
    # Initial MySQL
    conn = MySQLdb.connect( host = 'localhost', db = 'ccass_db', user = 'root', passwd = 'toor', charset = 'utf8', use_unicode = True)
    cursor = conn.cursor()

    # Select full stockid in broker_shares
    mysql_command = "select distinct StockID from broker_shares group by StockID"
    stock_id_list = enquiry_mysql(cursor, mysql_command)
    for stock_id in stock_id_list:
        table_name = 'HK' + stock_id[0]
        print table_name
        # mysql_command = "drop table if exists " + table_name
        # operate_mysql(conn, cursor, mysql_command)
        mysql_command = "create table " + table_name + " select * from broker_shares where StockID='" + stock_id[0] + "'"
        operate_mysql(conn, cursor, mysql_command)

    # Close MySQL
    conn.close()

def enquiry_mysql(cursor, mysql_command):
    cursor.execute(mysql_command)
    results = cursor.fetchall()
    return results

def operate_mysql(conn, cursor, mysql_command):
    cursor.execute(mysql_command)
    conn.commit()

if __name__ == "__main__":
    split_table()