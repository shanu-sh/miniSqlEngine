import sys
import sqlparse
import csv

tabledata={}
tablename=""
args=[]

def readfile(table):
    lines=[]
    f1=open(table)
    csvReader = csv.reader(f1)
    for row in csvReader:
        lines.append(row)
    f1.close()
    return lines

def getindex(table,column):
    colindex=-1
    for i in tabledata[table]:
        colindex=colindex+1
        if(i==column):
            return colindex
    return -1

def checktablepresent(table):
    for i in tabledata:
        if(i==table):
            return True
    return False
    
def readmetadata():
    f1=open("metadata.txt","r")
    l=f1.readlines()
    i=0
    while i<len(l):
        if l[i].strip()=='<begin_table>':

            if l[i+1].strip()!='<end_table>':
                tablename=l[i+1].strip()
                tabledata[tablename]=[]
                i=i+2
            while l[i].strip()!='<end_table>':
                tabledata[tablename].append(l[i].strip())
                i=i+1
            continue
        i=i+1
    f1.close()

def selectall(args):
    if(checktablepresent(args[3])):
        checktablepresent(args[3])
        line=readfile(args[3]+".csv")
        temp=""
        for i in tabledata[args[3]]:
            temp=temp+args[3]+'.'+i+','
        
        temp=temp[:-1]
        print(temp)
        
        for i in line:
            temp=""
            for j in i:
                temp=temp+j+","
            print(temp[:-1])
    else:
        print("table ",args[3]," not present")

def crossproduct(table1,table2):
    if(checktablepresent(table1)==False):
        print("table ",table1," not present")
        return
    if(checktablepresent(table2)==False):
        print("table ",table2," not present")
        return
    list1=readfile(table1+".csv")
    list2=readfile(table2+".csv")
    result=[]
    for i in list1:
        for j in list2:
            result.append(i+j)
    return result

def crossproductwithtable(list1,table2):
    if(checktablepresent(table2)==False):
        print("table ",table2," not present")
        return
    list2=readfile(table2+".csv")
    result=[]
    for i in list1:
        for j in list2:
            result.append(i+j)
    return result

def crossproductmany(tables): 
    result=crossproduct(tables[0],tables[1])
    if(result is None):
        return

    index=2
    while(index<len(tables)):
        result=crossproductwithtable(result,tables[index])
        index=index+1
    return result

def joinmany(tables):
    result=crossproductmany(tables)
    if(result is None):
        return
    t=""
    for i in tables:
        for j in tabledata[i]:
            t=t+i+"."+j+","
    print(t[:-1])

    for i in result:
        t=""        
        for j in i:
            t=t+j+","
        print(t[:-1])
    
def selectaggregate(args):
    params=args[1].split('(')
    cols=params[1].strip(')').split(',')

    colnum=-1
    data=[]
    flag=False
    if(len(cols)==1):
        if("." not in cols[0]):
            print("Please give proper syntax for column name")
            return
        if(checktablepresent(args[3])==False):
            print("table ",args[3]," not present")
            return
        l=readfile(args[3]+".csv")
        for i in tabledata[args[3]]:
            colnum=colnum+1
            # print(cols[0])
            table,column=cols[0].split(".")
            if(table!=args[3]):
                print("table name is different")
                return

            if i==column:
                print(args[3]+"."+i)
                flag=True
                break
        
        if(flag==False):
            print('No such column exist')
            return
        data=[ int(i[colnum]) for i in l]
        
        if(params[0].lower()=="max"):
            print(max(data))
        elif(params[0].lower()=="min"):
            print(min(data))
        elif(params[0].lower()=="sum"):
            print(sum(data))
        elif(params[0].lower()=="average"):
            print(sum(data)/len(data))
        else:
            print("command not recognized")
            
    else:
        print('Enter one column only')      
        
def selectdistinct(t_col,t_table):

    if(checktablepresent(args[3])==False):
        print("table ",args[3]," not present")
        return

    l=readfile(args[3]+".csv")
    colnum=-1
    flag=False

    if("." not in t_col[0]):
        print("Please give proper syntax for column name")
        return
        
    table,column=t_col[0].split(".")
    if(table!=t_table):
        print("table name is different")
        return
    
    for i in tabledata[t_table]:
        colnum=colnum+1
        if i==column:
            print(args[3]+"."+column)
            flag=True
            break
    
    if(flag==False):
        print('No such column exist')
        return
    
    data=set([ int(i[colnum]) for i in l])
    for i in data:
        print(i)

def selectonecolumn(t_cols,t_table):

    if(checktablepresent(t_table)==False):
        print("table ",t_table," not present")
        return

    l=readfile(t_table+".csv")
    t=""

    colmapping={}
    for i in t_cols:
        colnum=-1
        flag=False
        if("." not in i):
            print("Please give proper syntax for column name")
            return
        
        table,column=i.split(".")
        if(table!=t_table):
            print("table name is different")
            return
        
        for j in tabledata[t_table]:
            colnum=colnum+1
            if(column==j):
                colmapping[column]=colnum
                flag=True
                t=t+i+","
                break
        
        if(flag==False):
            print("Column ",i," is not present ")
            return
    print(t[:-1])
    for i in l:
        t=""
        for j in t_cols:
            table,column=j.split(".")
            t=t+i[colmapping[column]]+","
        print(t[:-1])

def selectmanycolums(cols,tables):
    colmapping={}
    t=""
    data=[]
    for i in cols:
        flag=False
        if("." not in i):
            print("Please give proper syntax for column name")
            return
        
        table,column=i.split(".")

        colnum=-1
        for k in tables:
            if(flag==False):
                for j in tabledata[k]:
                    colnum=colnum+1
                    if(column==j and table==k):
                        colmapping[column]=colnum
                        flag=True
                        t=t+k+"."+column+","
                        break

        if(flag==False):
            print("Column ",i," is not present ")
            return
       
    print(t[:-1])
    result=crossproductmany(tables)

    if( result is None):
        return
    for i in result:
        temp=""
        for j in cols:
            table,column=j.split(".")
            temp=temp+i[colmapping[column]]+","
        print(temp[:-1])

def splitonoperator(data):
    if("<=" in data):
        return data.split("<=")
    elif("<" in data):
        return data.split("<")
    elif(">=" in data):
        return data.split(">=")
    elif(">" in data):
        return data.split(">")
    elif("=" in data):
        return data.split("=")

def checkoperator(op,val1,val2):
    if("<=" in op):
        if(int(val1)<=int(val2)):
            return True
        else:
            return False
    elif("<" in op):
        if(int(val1)<int(val2)):
            return True
        else:
            return False
    elif(">=" in op):
        if(int(val1)>=int(val2)):
            return True
        else:
            return False
    elif(">" in op):
        if(int(val1)>int(val2)):
            return True
        else:
            return False
    elif("=" in op):
        if(int(val1)==int(val2)):
            return True
        else:
            return False

def printme(cols,tables,data,head):  
    if(data is None):
        return
    if(len(cols)==1 and cols[0]=="*"):
        print(head)
        for i in data:
            t=""
            for j in i:
                t=t+j+","
            print(t[:-1])

    else:
        colmapping={}
        for i in cols:
            if("." not in i):
                print("Please give proper syntax for column name")
                return
        
            table,column=i.split(".")
            flag=False
            colnum=-1
            for k in tables:
                if(flag==False):
                    for j in tabledata[k]:
                        colnum=colnum+1
                        if(j==column and table==k):
                            colmapping[column]=colnum
                            flag=True
                            break
                
            if(flag==False):
                print('Column ',column,' not present')
                return
        t=""
        for i in cols:
            t=t+i+","
        print(t[:-1])

        for i in data:
            t=""
            for j in cols:
                table,column=j.split(".")
                t=t+i[colmapping[column]]+","
            print(t[:-1])

def joinone(cols,tables,conditionlist):
    result=crossproductmany(tables)
    if(result is None):
        return
    colindex1=-1
    flag=False
    if("." in conditionlist[1]):

        tabledt1,columnval1=splitonoperator(conditionlist[1])
        table1,column1=tabledt1.split(".")

        for j in tables:
            if(flag==False):
                for i in tabledata[j]:
                    colindex1=colindex1+1
                    if(i==column1 and j==table1):
                        flag=True
                        break

        if(flag==False):
            print("Column is not present")
            return

        data=[]
        for t in result:   
            if(checkoperator(conditionlist[1],t[colindex1],columnval1)):
                data.append(t)

        t=""
        for i in tables:
            for j in tabledata[i]:
                t=t+i+"."+j+","

        printme(cols,tables,data,t[:-1])


def joinoncondition(cols,tables,conditionlist):
    condition=conditionlist[2]
    if(condition=="AND" or condition=="OR"):
        result=crossproductmany(tables)
        if(result is None):
            return
        colindex1=-1
        colindex2=-1

        if("." in conditionlist[1]):

            tabledt1,columnval1=splitonoperator(conditionlist[1])
            table1,column1=tabledt1.split(".")

            flag=False
            for j in tables:
                if(flag==False):
                    for i in tabledata[j]:
                        colindex1=colindex1+1
                        if(i==column1 and j==table1):
                            flag=True
                            break

            if(flag==False):
                print("Column is not present")
                return

        if("." in conditionlist[3]):

            tabledt2,columnval2=splitonoperator(conditionlist[3])
            table2,column2=tabledt2.split(".")

            flag=False
            for j in tables:
                if(flag==False):
                    for i in tabledata[j]:
                        colindex2=colindex2+1
                        if(i==column2 and j==table2):
                            flag=True
                            break

            if(flag==False):
                print("Column is not present")
                return
        
        data=[]
        for t in result:
            #print(t)
            if(condition=="AND"):
                if(checkoperator(conditionlist[1],t[colindex1],columnval1) and checkoperator(conditionlist[3],t[colindex2],columnval2)):
                    data.append(t)
            elif(condition=="OR"):
                if(checkoperator(conditionlist[1],t[colindex1],columnval1) or checkoperator(conditionlist[3],t[colindex2],columnval2)):
                    data.append(t)
        t=""
        for i in tables:
            for j in tabledata[i]:
                t=t+i+"."+j+","
        printme(cols,tables,data,t[:-1])
        
def joinwithtablenameonbothside(cols,tables,conditionlist,msg):
    result=crossproductmany(tables)
    if(result is None):
        return
    table1,column1=splitonoperator(conditionlist[1])[0].split(".")
    table2,column2=splitonoperator(conditionlist[1])[1].split(".")
    if(checktablepresent(table1)==False):
        print("table ",table1," not present")
        return
    if(checktablepresent(table2)==False):
        print("table ",table2," not present")
        return
    colindex1=getindex(table1,column1)
    colindex2=getindex(table2,column2)

    if(int(colindex1)==-1 or int(colindex2)==-1):
        print("column not present")
        return
    data=[]
    colnum=-1
    head=""
    if(">=" in conditionlist[1] or ">" in conditionlist[1] or "<=" in conditionlist[1] or "<" in conditionlist[1]):
        for i in tables:
            for j in tabledata[i]:
                head=head+i+"."+j+","

        for j in result:
            colnum=-1
            t=[]
           
            if(checkoperator(conditionlist[1],j[colindex1],j[colindex2+len(tabledata[table1])])):
                data.append(j)

    else:

        for j in result:
            colnum=-1
            t=[]
           
            if(checkoperator(conditionlist[1],j[colindex1],j[colindex2+len(tabledata[table1])])):
                for i in j:
                    colnum=colnum+1
                    if(colnum!=colindex2+len(tabledata[table1])):
                        t.append(i)
                data.append(t)

        for i in tables:
            for j in tabledata[i]:
                head=head+i+"."+j+","
  
    if(msg):
        printme(cols,tables,data,head[:-1])
    else:
        t=[]
        
        data.append(head[:-1])
        return data

def joinoneconditionandtablename(cols,tables,conditionlist):
    data=joinwithtablenameonbothside(cols,tables,conditionlist[0:2],False)
    if(data is None):
        return
    head=data[len(data)-1]
    data=data[:-1]

    tabledt,columnval=splitonoperator(conditionlist[3])
    table,columnname=tabledt.split(".")
    flag=False

    if(checktablepresent(table)==False):
        print("table ",table," not present")
        return

    for i in tabledata[table]:
        if(i==columnname):
            flag=True
            break

    if(flag==False):
        print('Column of second column not present')
        return

    colindex=-1
    for i in head.split(","):
        colindex=colindex+1
        if(columnname==i.split(".")[1]):
            break

    result=[]
    for i in data:
        if(checkoperator(conditionlist[3],i[colindex],columnval)):
            result.append(i)

    printme(cols,tables,result,head)

def conditionononetable(cols,tables,conditionlist):
    if(checktablepresent(tables[0])==False):
        print("table ",tables[0]," not present")
        return

    lines=readfile(tables[0]+".csv")
    table1,column1=splitonoperator(conditionlist[1])[0].split(".")
    colindex=getindex(table1,column1)
    val=splitonoperator(conditionlist[1])[1]
    data=[]
    for i in lines:
        if(checkoperator(conditionlist[1],i[colindex],val)):
            data.append(i)

    t=""
    for i in tabledata[tables[0]]:
        t=t+tables[0]+"."+i+","
    printme(cols,tables,data,t[:-1])

def twoconditionononetable(cols,tables,conditionlist):
    if(checktablepresent(tables[0])==False):
        print("table ",tables[0]," not present")
        return

    lines=readfile(tables[0]+".csv")
    table1,column1=splitonoperator(conditionlist[1])[0].split(".")
    table2,column2=splitonoperator(conditionlist[3])[0].split(".")

    colindex1=getindex(table1,column1)
    colindex2=getindex(table2,column2)

    val1=splitonoperator(conditionlist[1])[1]
    val2=splitonoperator(conditionlist[3])[1]
    data=[]
    for i in lines:
        if("AND" in conditionlist[2]):
            if(checkoperator(conditionlist[1],i[colindex1],val1) and checkoperator(conditionlist[3],i[colindex2],val2)):
                data.append(i)
        elif("OR" in conditionlist[2]):
            if(checkoperator(conditionlist[1],i[colindex1],val1) or checkoperator(conditionlist[3],i[colindex2],val2)):
                data.append(i)
        else:
            print("Operation not supported")

    t=""
    for i in tabledata[tables[0]]:
        t=t+tables[0]+"."+i+","

    printme(cols,tables,data,t[:-1])

def processquery(args):
    
    aggregate=args[1].split('(')[0].lower()
    tables=args[3].split(',')

    
    if(len(args)==4 and args[0].lower()=="select" and args[1]=="*" and len(tables)==1):
        selectall(args)

    elif(len(args)==4 and args[0].lower()=="select" and args[1]=="*" and len(tables)>=2):
        joinmany(tables)

    elif(len(args)==4 and args[0].lower()=="select" and (aggregate=="max" or aggregate=="min" or aggregate=="sum" or aggregate=="average") and len(tables)==1 ):
        selectaggregate(args)

    elif(len(args)==4 and args[0].lower()=="select" and aggregate=="distinct" and len(tables)==1):
        params=args[1].split('(')
        cols=params[1].strip(')').split(',')
        if(len(cols)==1):
            selectdistinct(cols,args[3])
        else:
            print("Please specify one column only")

    elif(len(args)==4 and args[0].lower()=="select" and len(tables)>1):
        params=args[1].split('(')
        cols=params[0].split(',')
        selectmanycolums(cols,tables)

    elif(len(args)==4 and args[0].lower()=="select" and len(tables)==1):
        params=args[1].split('(')
        cols=params[0].split(',')
        selectonecolumn(cols,tables[0])

    elif(len(args)==5 and args[0].lower()=="select" and len(tables)>=2):
        params=args[1].split('(')
        cols=params[0].split(',')
        listcond=args[4].split()

        if(len(cols)>=1):
            if(len(listcond)==4):
                if("." in splitonoperator(listcond[1])[0] and "." in splitonoperator(listcond[1])[1] and 
                    "." in splitonoperator(listcond[3])[0] and "." in splitonoperator(listcond[3])[1] ):
                    print("")

                elif("." in splitonoperator(listcond[1])[0] and "." in splitonoperator(listcond[1])[1]):
                    joinoneconditionandtablename(cols,tables,listcond)
                else:
                    joinoncondition(cols,tables,listcond)
            elif(len(listcond)==2):
                if("." in splitonoperator(listcond[1])[0] and "." in splitonoperator(listcond[1])[1]):
                    joinwithtablenameonbothside(cols,tables,listcond,True)
                else:
                    joinone(cols,tables,listcond)
                print()
            else:
                print("Please specify proper conditions")
        else:
            print("Please specify at least one column")

    elif(len(args)==5 and args[0].lower()=="select" and len(tables)==1):
        params=args[1].split('(')
        cols=params[0].split(',')
        listcond=args[4].split()
        
        if(len(listcond)==4):
            twoconditionononetable(cols,tables,listcond)

        elif(len(listcond)==2):
            conditionononetable(cols,tables,listcond)

        
input=sys.argv[1]
t=sqlparse.sql.IdentifierList(sqlparse.parse(input)[0].tokens).get_identifiers()

for i in t:
    args.append(str(i))

try:
    readmetadata()
except:
    print("Unable to read meta data")

try:
    processquery(args)
except:
    print("Please give proper syntax")
