import psycopg2
import os
USE_BAO = True
# USE_BAO = False
PG_CONNECTION_STR = "dbname=imdb user=imdb host=localhost"

def run_query(sql, bao_select=False, bao_reward=False):

    try:
        conn = psycopg2.connect(PG_CONNECTION_STR)
        cur = conn.cursor()
        cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
        cur.execute(f"SET enable_bao_selection TO {bao_select}")
        cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
        cur.execute("SET bao_num_arms TO 5")
        cur.execute("SET statement_timeout TO 300000")
        cur.execute(sql)
        cur.fetchall()
        conn.close()
        
        # NOTE: save sql query
        with open("/home/slm/pg_related/BaoForPostgreSQL/query_log/sql.txt","a") as f:
            f.write(sql+"\n")
            

    except psycopg2.Error as e:
        print(e.pgerror)
        
if __name__ == "__main__":
    sql = "select * from title"
    run_query(sql,bao_select=USE_BAO, bao_reward=USE_BAO)