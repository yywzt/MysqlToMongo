import torndb_for_python3
import pymongo

# 连接mysql
mysql_conn = torndb_for_python3.Connection(host='192.168.2.172', database='fd_pay_center', user='root', password='jnqn3@vyw', charset='utf8')

# 连接mongodb
client = pymongo.MongoClient(host='localhost',port=27017,username='root',password='root',authSource='admin',authMechanism='SCRAM-SHA-1')
# 数据库
db = client.test
# 集合
collection = db.sfy_pos_order



def query(start,end):
    sfy_pos_order = mysql_conn.query('SELECT id as mysqlId,	order_no,	sub_order_no,	pay_order_no,	pay_channel,	CAST(pay_money AS CHAR(50)) AS pay_money,	'
                                     'pay_state,	pay_time,	payee,	payer,	payer_phone,	house_id,	house_no,	building_id,	building_name,	village_id,	'
                                     'village_name,	business_type,	item_id,	item_name,	CAST(money AS CHAR(50)) AS money,	CAST(late_fee AS CHAR(50)) AS last_fee,	'
                                     'fee_id,	cost_peroid,	current_cost_peroid,	remark,	sync_flag,	sync_msg,	sync_url,	sync_fail_time,	ticket,	creation_date,	'
                                     'created_by,	updation_date,	updated_by,	enabled_flag,	refund_flag,	refund_sync_flag,	refund_sync_msg,	refund_sync_fail_time,'
                                     '	refund_time FROM sfy_pos_order limit %s,%s'%(start,end))
    return sfy_pos_order

#查询总数
count = mysql_conn.query('SELECT count(1) as count FROM sfy_pos_order')
print(count)
2906255

n=0
while n<291:
    sfy_pos_order = query(n*10000,10000)
    print(sfy_pos_order)
    collection.insert_many(sfy_pos_order)
    n=n+1

# 查询mysql的一条记录并插入至mongodb
result = query(0,1)
print(result)
collection.insert_many(result)