import psycopg2

class pg:

  Host=''
  Database=''
  User=''
  Port=''
  Password=''
  Conn = None

  def __init__(Self, host, database, user, port, password):
    Self.Host= host
    Self.Database= database
    Self.User = user
    Self.Port = port
    Self.Password = password
    Self.Conn = Self.conecta_pg()

  def __del__(Self):
    Self.Conn.close()
    

  def conecta_pg(Self):
      return psycopg2.connect(
      host=Self.Host,
      database=Self.Database,
      user=Self.User,
      port=Self.Port,
      password=Self.Password)

  def executa_sql(Self, sql):
      cur = Self.Conn.cursor()
      cur.execute(sql)
      Self.Conn.commit()

  def consultar_db(Self, sql):
    cur = Self.Conn.cursor()
    cur.execute(sql)
    recset = cur.fetchall()
    registros = []
    for rec in recset:
      registros.append(rec)
    return registros


