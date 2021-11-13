import re

f = open("data.csv", "r")

def task1(name):
    f = open(name, "r")
    count_dic = {}
    l = f.readline()
    while True:
        name = ", ".join(l.split(",")[:2]).strip()
        count_dic[name] = count_dic.get(name, 0) + 1
        l = f.readline()
        if not l:
            break
    
    return count_dic

def task2(name):
    f = open(name, "r")
    count_dic = {}
    l = f.readline()
    visiteelast_i = l.split(",").index("visitee_namelast")
    while True:
        info = l.split(",")
        if info[visiteelast_i] or info[visiteelast_i + 1]:
            name = ", ".join(info[visiteelast_i:visiteelast_i + 2]).strip(", ")
            count_dic[name] = count_dic.get(name, 0) + 1
        
        l = f.readline()
        if not l:
            break
    
    return count_dic

def task3(name):
    f = open(name, "r")
    count_dic = {}
    final_set = set()
    l = f.readline()
    year_i = l.split(",").index("APPT_START_DATE")
    while True:
        info = l.split(",")
        year = re.search(r"2009|2010", info[year_i]) # only pulls 2009 or 2010
        name = ", ".join(info[:2]).strip(", ")
        if year and name != ",":  # because we stripped, empty string will appear as ","
            count_dic[name] = count_dic.get(name, []) + [year]

        l = f.readline()
        if not l:
            break
    
    for key, value in count_dic.items():
        if len(set(value)) > 1: 
            final_set.add(key)

    return final_set


