from rrpartition.store import connector

def insert(value):
    conn,cur = connector()
    cur.execute("USE ads5_1;")
    name = "pr"
    price = 0
    seller = "sel"
    category = "cat"
    for i in range(value):
        cur.execute(f"INSERT INTO product(name,price,seller,type) VALUES('{name+str(i)}',{price+i},'{seller+str(i)}','{category+str(i)}');")
    conn.commit()

insert(50)