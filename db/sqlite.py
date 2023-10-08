import sqlite3

class sqlite:

    conn = sqlite3.connect('clientes.db')
    cursor = conn.cursor()
    def __init__(self) -> None:
        pass

    def query(self, sql)->list:
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def __del__(self):
        self.conn.close()




