# -*- coding: utf-8 -*-

from scrapy.conf import settings
from datetime import date, timedelta
import MySQLdb
import pandas

def broker_info(stockid, start_date, end_date, top_number):
    # Initial MySQL
    conn = MySQLdb.connect( host = settings.get('MYSQL_HOST'),
                            db = settings.get('CCASS_DB'),
                            user = settings.get('MYSQL_USER'), 
                            passwd = settings.get('MYSQL_PASSWD'),
                            charset = 'utf8',
                            use_unicode = True
                            )
    cursor = conn.cursor()

    # Structure Sub_Table
    table_name = 'HK' + stockid
    subtable_name = 'tub_table'
    mysql_command = "drop table if exists " + subtable_name
    operate_mysql(conn, cursor, mysql_command)
    mysql_command = "create table " + subtable_name + " select * from " + table_name + " where StockID='"
    mysql_command = mysql_command + stockid + "' and Date between '" + start_date + "' and '" + end_date + "'"
    operate_mysql(conn, cursor, mysql_command)

    # Select full broker id between days
    mysql_command = "select distinct Broker_ID from " + subtable_name
    br_id_list = enquiry_mysql(cursor, mysql_command)

    # Structure OutPut 
    data_columns = ['Broker_ID',
                    'Broker_Name',                    
                    start_date,
                    end_date,
                    'Diff',
                    # 'Min_Shares',
                    # 'Max_Shares',
                    'Diff(Max-Min)']     
    br_data = pandas.DataFrame(columns = data_columns)
    for br_id in br_id_list:
        # Broker_Name
        mysql_command = "select distinct Broker_Name from " + subtable_name + " where Broker_ID='" + br_id[0] + "'"
        br_name = enquiry_mysql(cursor, mysql_command)
        broker_name = br_name[0][0]        
        # Start - End Date
        mysql_command = "select Shares from " + subtable_name + " where Broker_ID='" + br_id[0] + "' and Date='" + start_date + "'"
        d_value = enquiry_mysql(cursor, mysql_command)
        if (len(d_value) == 0):
            start_value = 0
        else:
            start_value = d_value[0][0]
        mysql_command = "select Shares from " + subtable_name + " where Broker_ID='" + br_id[0] + "' and Date='" + end_date + "'"
        d_value = enquiry_mysql(cursor, mysql_command)
        if (len(d_value) == 0):
            end_value = 0
        else:
            end_value = d_value[0][0]
        diff_value = end_value - start_value
        # Min_Max
        mysql_command = "select min(Shares), max(Shares) from " + subtable_name + " where Broker_ID='" + br_id[0] + "'"
        m_value = enquiry_mysql(cursor, mysql_command)
        min_value = m_value[0][0]
        if end_value == 0:
            min_value = end_value
        if start_value == 0:
        	min_value = start_value
        max_value = m_value[0][1]
        mdiff_value = max_value - min_value  
        br_data.loc[len(br_data)] = [
                br_id[0],
                broker_name,                
                float(start_value),
                float(end_value),
                float(diff_value),
                # float(min_value),
                # float(max_value),
                float(mdiff_value)]
    # Display Setup
    pandas.set_option('expand_frame_repr', False) # in one line
    pandas.set_option('display.float_format' , '{:,.0f}'.format) # float format
    # Display Results
    print '------------------------'
    print 'Stock ID: ' + stockid
    print '------------------------'
    print br_data.sort_values(['Diff(Max-Min)',end_date], ascending=[False, False]).head(top_number)   

    # Close MySQL
    mysql_command = "drop table " + subtable_name
    operate_mysql(conn, cursor, mysql_command)
    conn.close()


def enquiry_mysql(cursor, mysql_command):
    cursor.execute(mysql_command)
    results = cursor.fetchall()
    return results

def operate_mysql(conn, cursor, mysql_command):
    cursor.execute(mysql_command)
    conn.commit()

if __name__ == "__main__":
    broker_info('01194', '2017-09-30', '2017-10-13', 20)