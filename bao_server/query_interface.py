# %% 
import numpy as np
import os
import copy
from itertools import permutations
# %%
def parse_table(table_part):
    """parse tables and shortcuts of tables from query

    Args:
        table_part ([str]): 
    NOTE: There might be one table occurs multiple times, not sure if pg_hint_plan supports
    """
    tables = []
    table_shortcuts = []
    tables_all = table_part.split(",")
    
    for each in tables_all:
        if("AS" in each):
            tables.append(each.strip().split(" AS ")[0])
            table_shortcuts.append(each.strip().split(" AS ")[1])
        elif("as" in each):
            tables.append(each.strip().split(" as ")[0])
            table_shortcuts.append(each.strip().split(" as ")[1])

    return tables, table_shortcuts

def parse_predicates(predicates_part, table_shortcuts):
    """parse join and scan predicates from query

    Args:
        predicates_part ([str]): [sql query]
        table_shortcuts ([list]): [tables of the query]

    Returns:
        [list]: [joins and predicates]
    """
    joins = []
    predicates = []
    _predicates = predicates_part.split("AND")
    for each in _predicates:
        tmp = each.strip()
        if("=" not in each):
            predicates.append(tmp)
        elif("=" in tmp and "(" in tmp):
            predicates.append(tmp)
        elif("=" in tmp and "'" in tmp):
            predicates.append(tmp)
        else:
            contents = tmp.split(" = ")
            if(len(contents)!=2):
                predicates.append(tmp)
            else:
                for content in contents:
                    if(content.split(".")[0] not in table_shortcuts):
                        predicates.append(tmp)
                        break
                    joins.append(tmp)
    # print("Joins: ", joins)
    # print("Predicates: ", predicates)
    return joins, predicates
                    
def parse_sql(sql):
    """parse a sql

    Args:
        sql ([type]): [description]

    Returns:
        [type]: [description]
    """
    assert sql.count("FROM") == 1, "nested SQL query"
    table_part = sql.split("FROM")[1].split("WHERE")[0]
    tables, table_shortcuts = parse_table(table_part)
    
    predicates_part = sql.split("WHERE")[1]
    joins, predicates = parse_predicates(predicates_part,table_shortcuts)
    
    return tables, table_shortcuts, joins, predicates


def cartesian(arrays, dtype=None, out=None):
    arrays = [np.asarray(x) for x in arrays]
    if dtype is None:
        dtype = arrays[0].dtype
    n = np.prod([x.size for x in arrays])
    if out is None:
        out = np.zeros([n, len(arrays)], dtype=dtype)

    m = int(n / arrays[0].size) 
    out[:,0] = np.repeat(arrays[0], m)
    if arrays[1:]:
        cartesian(arrays[1:], out=out[0:m, 1:])
        for j in range(1, arrays[0].size):
            out[j*m:(j+1)*m, 1:] = out[0:m, 1:]
    return out

def generate_scan_hints(tables):
    scan_methods = ["SeqScan({})", "IndexScan({})"]
    hint_candidate = []
    for table in tables:
        table_candidate =[]
        for method in scan_methods:
            table_candidate.append(method.format(table))
        hint_candidate.append(table_candidate)
    candidates = [" ".join(x) for x in cartesian(hint_candidate, 'object')]
    return candidates

def generate_join_method_hints(tables):
    join_methods = ["NestLoop({})","MergeJoin({})","HashJoin({})"]
    if(tables==[""]):
        return [""]
    hint_candidate = []
    for join in tables:
        join_table = [x.split(".")[0] for x in join.split("=")]
        join_candidate = [each.format(" ".join(join_table)) for each in join_methods]
        hint_candidate.append(join_candidate)
    candidates = [" ".join(x) for x in cartesian(hint_candidate, 'object')]
    return candidates

def add_one_rel(cur, join_tables):
    extended_order = []
    for table in join_tables:
        if(table not in cur):
            tmp = ["("]
            tmp.extend(cur)
            tmp.append(table)
            tmp.append(")")
            extended_order.append(tmp)

            tmp = ["("]
            tmp.append(table)            
            tmp.extend(cur)
            tmp.append(")")

            extended_order.append(tmp)
        else:
            continue
    return extended_order

def parse_order(order):
    left = 0
    right = len(order) - 1
    parsed_order = []
    while(left<right):
        if(order[left]=="(" and order[right]==")"):
            left += 1
            right -= 1
        elif(order[left]=="("):
            parsed_order.insert(0,order[right])
            right -= 1
        elif(order[right]==")"):
            parsed_order.insert(0,order[left])
            left += 1
        else:
            parsed_order.insert(0,order[right])
            parsed_order.insert(0,order[left])
            left += 1
            right -= 1
    return parsed_order

def generate_join_order_hins(tables):
    if(len(tables)==1):
        return [""],[]
    join_tables = tables
    num_tables = len(tables)
    str_order_length = 3*num_tables-2
    join_orders = []
    starter = copy.deepcopy(join_tables)
    stack = [[each] for each in starter]
    while(len(stack)!=0):
        cur = stack.pop(0)
        if(len(cur)<str_order_length):
            extended_orders = add_one_rel(cur, join_tables)
            stack.extend(extended_orders)
        else:
            join_orders.append(cur)
    str_join_orders = [" ".join(each) for each in join_orders]
    # print(str_join_orders)
    str_join_orders = set(str_join_orders)
    join_orders_string = ["Leading ({})".format(each) for each in str_join_orders]
    # print(join_orders)
    return join_orders_string, join_orders

def generate_join_method_hints_from_orders(join_order_hints, join_orders_list):
    join_methods = ["NestLoop({})","MergeJoin({})","HashJoin({})"]
    join_hints = []
    for order_hint, order in zip(join_order_hints,join_orders_list):
        parsed_order = parse_order(order)
        join_order = []
        for idx in range(2,len(parsed_order)+1):
            join_order.append(" ".join(parsed_order[0:idx]))
        join_candidate = []
        for idx,level in enumerate(join_order):
            join_candidate.append([each.format(level) for each in join_methods])
        candidates = [" ".join(x) for x in cartesian(join_candidate, 'object')]
        join_hints.extend([each + " " + order_hint for each in candidates])
    if(join_hints==[]):
        join_hints = [""]
    return join_hints

def generate_hints(sql):
    """generate potential hints for sql query

    Args:
        sql ([str]): [SQL query in format select * from * where *]
    """
    tables, table_shortcuts, joins, predicates = parse_sql(sql)
    scan_hints = generate_scan_hints(table_shortcuts)
    join_order_hints, join_orders = generate_join_order_hins(table_shortcuts)
    join_hints = generate_join_method_hints_from_orders(join_order_hints,join_orders)
    candidates = [scan_hints, join_hints]
    hints_set = [" ".join(x) for x in cartesian(candidates, 'object')]
    queries = []
    for each in hints_set:
        query = "/*+ {} */ ".format(each) + sql + ";"
        queries.append(query)
    return queries, sql+";"

if __name__=="__main__":
    sample_queries = os.listdir("/home/slm/pg_related/BaoForPostgreSQL/sample_queries")
    for each in sample_queries:
        print(each)
        with open(os.path.join("/home/slm/pg_related/BaoForPostgreSQL/sample_queries",each), "r") as f:
            # sql = f.read()
            sql = "explain select count(*) FROM title AS t, movie_info AS mi, movie_info_idx AS mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND mi.info_type_id > 16 AND mi_idx.info_type_id = 100;"
            queries_with_hint, sql = generate_hints(sql)
            for each in queries_with_hint:
                print(each)
        break
# %%
