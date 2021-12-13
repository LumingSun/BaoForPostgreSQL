import psycopg2
import os
import csv
import json
# USE_BAO = True
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
        res = cur.fetchall()
        conn.close()
        # print(res)
        with open("/home/slm/pg_related/BaoForPostgreSQL/query_log/query_result.txt","w") as f:
            csv_out = csv.writer(f)
            csv_out.writerows(res)
            
        #save sql query
        with open("/home/slm/pg_related/BaoForPostgreSQL/query_log/sql.txt","a") as f:
            f.write(sql+"\n")
        print("Query executed successfully")
        
        # run explain and save plan
        conn = psycopg2.connect(PG_CONNECTION_STR)
        cur = conn.cursor()
        cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
        cur.execute(f"SET enable_bao_selection TO {bao_select}")
        cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
        cur.execute("SET bao_num_arms TO 5")
        cur.execute("SET statement_timeout TO 300000")
        cur.execute("explain " + sql)
        res = cur.fetchall()
        conn.close()
        sql_count = len(os.listdir("/home/slm/pg_related/BaoForPostgreSQL/query_log/plan_log/"))
        with open("/home/slm/pg_related/BaoForPostgreSQL/query_log/plan_log/{}.csv".format(sql_count),"w") as f:
            json.dump(res, f, ensure_ascii=False)
            
        return True

    except psycopg2.Error as e:
        print(e.pgerror)
        with open("/home/slm/pg_related/BaoForPostgreSQL/query_log/query_result.txt","w") as f:
            f.writelines(e.pgerror)
        
        return False
        
        
def optimize_query(sql, bao_select=True, bao_reward=True):

    try:
        sql_count = len(os.listdir("/home/slm/pg_related/BaoForPostgreSQL/query_log/plan_log/"))
        conn = psycopg2.connect(PG_CONNECTION_STR)
        cur = conn.cursor()
        cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
        cur.execute(f"SET enable_bao_selection TO {bao_select}")
        cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
        cur.execute("SET bao_num_arms TO 5")
        cur.execute("SET statement_timeout TO 300000")
        cur.execute("explain " + sql)
        conn.close()
        # print(res)
        sql_count_after = len(os.listdir("/home/slm/pg_related/BaoForPostgreSQL/query_log/plan_log/"))
        for i in range(sql_count,sql_count_after):
            os.remove("/home/slm/pg_related/BaoForPostgreSQL/query_log/plan_log/{}.csv".format(i))

        print("Query optimized")    
        
        return True

    except psycopg2.Error as e:
        print(e.pgerror)
        with open("/home/slm/pg_related/BaoForPostgreSQL/query_log/query_result.txt","w") as f:
            f.writelines(e.pgerror)
        
        return False