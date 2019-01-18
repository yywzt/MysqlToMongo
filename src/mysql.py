import pymysql
import torndb_for_python3

# pymysql连接mysql方式
conn = pymysql.connect(host='192.168.2.172', database='fd_pay_center', user='root', password='jnqn3@vyw', charset='utf8')
cursor = conn.cursor();
#返回受影响行数
count = cursor.execute('select CAST(pay_money AS CHAR(50)) from sfy_pos_order limit 1000')
# result = cursor.fetchall();
cursor.rownumber=0
page = cursor.fetchmany(10)
print(count)
print(page)

for rr in page:
    print(rr)

# torndb_for_python3连接mysql方式
mysql_conn = torndb_for_python3.Connection(host='192.168.2.172', database='fd_pay_center', user='root', password='jnqn3@vyw', charset='utf8')
def query(start,end):
    sfy_pos_order = mysql_conn.query('SELECT CAST(pay_money AS CHAR(50)) as money FROM sfy_pos_order limit %s,%s'%(start,end))
    return sfy_pos_order

result = query(0,10)
print(result)