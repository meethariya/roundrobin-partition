import mysql.connector as MYSQL

def connector():
    conn = MYSQL.connect(host='localhost',user = 'root',password = 'root', database = 'ads5_1')
    cur = conn.cursor()
    return conn, cur

def rrpartition(n):
    conn, cur = connector()
    cur.execute('''SELECT count(PARTITION_NAME)
        FROM INFORMATION_SCHEMA.PARTITIONS  
        WHERE TABLE_SCHEMA = 'ads5_1' AND TABLE_NAME = 'product';''')
    ps = cur.fetchone()[0]
    if ps != 0:
        return {
            'alert' : True,
            'result' : "danger",
            'outcome' : "Failed",
            'outcome_message' : "Database already partitioned."}
    cur.execute(f"alter table product PARTITION BY HASH(id) PARTITIONS {n};")
    conn.commit()
    dictionary = {
            'alert' : True,
            'result' : "success",
            'outcome' : "Success",
            'outcome_message' : "Database partitioned successfully!"}
    return dictionary

def infor():
    info = []
    conn, cur = connector()

    cur.execute('''SELECT count(PARTITION_NAME)
        FROM INFORMATION_SCHEMA.PARTITIONS  
        WHERE TABLE_SCHEMA = 'ads5_1' AND TABLE_NAME = 'product';''')
    temp = cur.fetchone()
    if temp[0] != 0:
        cur.execute('''SELECT PARTITION_NAME
            FROM INFORMATION_SCHEMA.PARTITIONS  
            WHERE TABLE_SCHEMA = 'ads5_1' AND TABLE_NAME = 'product';''')
        ps = cur.fetchall()
        ps = [j[0] for j in ps]
        ps.sort()
        ps = ps[1:]+ps[:1]
        for i in ps:
            # fetching col names
            cur.execute(f'''SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'product' AND TABLE_SCHEMA='{i}';''')
            cols=cur.fetchall()
            cols = [j[0] for j in cols]
            cur.execute(f"SELECT * FROM product PARTITION ({i});")
            rows = cur.fetchall()
            obj = {
                'partition':i,
                'table':'product',
                'cols': cols,
                'rows': rows
            }
            info.append(obj)
    else:
        # fetching col names
        cur.execute(f'''SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'product' AND TABLE_SCHEMA='ads5_1';''')
        cols=cur.fetchall()
        cols = [j[0] for j in cols]
        cur.execute("select * from ads5_1.product;")
        rows= cur.fetchall()
        obj = {
                'partition':'None',
                'table':'product',
                'cols': cols,
                'rows': rows
            }
        info.append(obj)

    size = 8 if len(info) == 1 else 4
    dictionary = {
        'info':info,
        'size':size
    }
    return dictionary

def reset_partition():
    conn, cur = connector()
    dictionary = infor()
    cur.execute("DROP TABLE ads5_1.product;")
    cur.execute('''
        create table product(
        id int primary key auto_increment,
        name varchar(100) not null,
        price int not null,
        seller varchar(100) not null,
        type varchar(100) not null);
    ''')
    info = dictionary['info']
    rows = []
    for i in info:
        rows+=i['rows']
    cur.executemany(f"insert into ads5_1.product (id,name,price,seller,type) values (%s,%s,%s,%s,%s);",rows)
    conn.commit()
    conn.close()
    dictionary = {
            'alert' : True,
            'result' : "success",
            'outcome' : "Success",
            'outcome_message' : "Database reset successfully!"}
    return dictionary