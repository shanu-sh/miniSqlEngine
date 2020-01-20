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
    print(tabledata)
    f1.close()

def selectall(args):    
    line=readfile(args[3]+".csv")
    temp=""
    for i in tabledata[args[3]]:
        temp=temp+args[3]+'.'+i+'\t'
    
    temp=temp[:-1]
    print(temp)
    
    for i in line:
        for j in i:
            print(j,end='\t\t')
        print('\n')

def crossproduct(table1,table2):
    list1=readfile(table1+".csv")
    list2=readfile(table2+".csv")
    result=[]
    for i in list1:
        for j in list2:
            result.append(i+j)
    return result

def crossproductmany(tables): 
    result=crossproduct(tables[0],tables[1])
    f1=open("temp.csv","w")
    for i in result:
        t=[]
        for j in i:
            t.append(j)
        temp=""
        for k in t:
            temp=temp+k+","
        temp=temp[:-1]
        f1.write(temp+"\n")    
    f1.close()

    index=2
    while(index<len(tables)):
        result=crossproduct("temp",tables[index])
        index=index+1
        f1=open("temp.csv","w")
        for i in result:
            t=[]
            for j in i:
                t.append(j)
            temp=""
            for k in t:
                temp=temp+k+","
            temp=temp[:-1]
            f1.write(temp+"\n")
            
        f1.close()

    return result

def joinmany(tables):
    result=crossproductmany(tables)
    t=""
    for i in tables:
        for j in tabledata[i]:
            t=t+i+"."+j+"\t"
    print(t)

    for i in result:        
        for j in i:
            print(j,end="\t\t")
        print()
    
def selectaggregate(args):
    params=args[1].split('(')
    cols=params[1].strip(')').split(',')

    colnum=-1
    data=[]
    flag=False
    if(len(cols)==1):

        l=readfile(args[3]+".csv")
        for i in tabledata[args[3]]:
            colnum=colnum+1
            if i==cols[0]:
                print(args[3]+"."+i)
                flag=True
                break
        
        if(flag==False):
            print('No such column exist')
            return
        data=[ int(i[colnum]) for i in l]
        
        if(params[0]=="max"):
            print(max(data))
        elif(params[0]=="min"):
            print(min(data))
        elif(params[0]=="sum"):
            print(sum(data))
        elif(params[0]=="average"):
            print(sum(data)/len(data))
        else:
            print("command not recognized")
            
    else:
        print('Enter one column only')      
        
def selectdistinct(t_col,t_table):
    l=readfile(args[3]+".csv")
    colnum=-1
    flag=False

    for i in tabledata[t_table]:
        colnum=colnum+1
        if i==t_col[0]:
            print(args[3]+"."+i)
            flag=True
            break
    
    if(flag==False):
        print('No such column exist')
        return
    
    data=set([ int(i[colnum]) for i in l])
    for i in data:
        print(i)

def selectonecolumn(t_cols,t_table):
    l=readfile(t_table+".csv")
    t=""
    colmapping={}
    for i in t_cols:
        colnum=-1
        flag=False
        for j in tabledata[t_table]:
            colnum=colnum+1
            if(i==j):
                colmapping[i]=colnum
                flag=True
                t=t+t_table+"."+i+"\t"
                break
        
        if(flag==False):
            print("Column ",i," is not present ")
            return
    print(t)
    for i in l:
        t=""
        for j in t_cols:
            t=t+i[colmapping[j]]+"\t\t"
        print(t)

def selectmanycolums(cols,tables,msg):
    colmapping={}
    t=""
    data=[]
    for i in cols:
        flag=False
        colnum=-1
        for k in tables:
            if(flag==False):
                for j in tabledata[k]:
                    colnum=colnum+1
                    if(i==j):
                        colmapping[i]=colnum
                        flag=True
                        t=t+k+"."+i+"\t"
                        break

        if(flag==False):
            print("Column ",i," is not present ")
            return
    if(msg):    
        print(t)
    result=crossproductmany(tables)

    for i in result:
        temp=""
        for j in cols:
            if(msg):
                print(i[colmapping[j]],end="\t\t")
            temp=temp+i[colmapping[j]]+" "
            
        if(msg):
            print()
        data.append(temp)

    return data

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

def operator(data,val): 
    if("<=" in data):
        if(int(data.split("<=")[1])>=int(val)):
            return True
        else:
            return False
    elif("<" in data):
        if(int(data.split("<")[1])>int(val)):
            return True
        else:
            return False
    elif(">=" in data):
        if(int(data.split(">=")[1])<=int(val)):
            return True
        else:
            return False
    elif(">" in data):
        if(int(data.split(">")[1])<int(val)):
            return True
        else:
            return False
    elif("=" in data):
        if(int(data.split("=")[1])==int(val)):
            return True
        else:
            return False

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

def printme(cols,tables,data):       
    if(len(cols)==1 and cols[0]=="*"):
        for i in data:
            for j in i:
                print(j,end='\t')
            print()

    else:
        colmapping={}
        for i in cols:
            flag=False
            colnum=-1
            for k in tables:
                if(flag==False):
                    for j in tabledata[k]:
                        colnum=colnum+1
                        if(j==i):
                            colmapping[i]=colnum
                            flag=True
                            break
                
            
            if(flag==False):
                print('Column ',i,' not present')
                return

        for i in data:
            for j in cols:
                print(i[colmapping[j]],end='\t')
            print()

def joinone(cols,tables,conditionlist):
    print("joinone")
    result=crossproductmany(tables)
    colindex1=-1
    flag=False
    if("." in conditionlist[1]):

        if("=" in conditionlist[1]):
            table1=conditionlist[1].split('=')[0].split(".")[0]
            column1=conditionlist[1].split('=')[0].split(".")[1].strip("<").strip(">")

        elif("<" in conditionlist[1]):
            table1=conditionlist[1].split('<')[0].split(".")[0]
            column1=conditionlist[1].split('<')[0].split(".")[1]

        elif(">" in conditionlist[1]):
            table1=conditionlist[1].split('>')[0].split(".")[0]
            column1=conditionlist[1].split('>')[0].split(".")[1]

        print(column1,table1)
        for j in tables:
            print("table is ",j)
            if(flag==False):
                for i in tabledata[j]:
                    colindex1=colindex1+1
                    print("column is ",i)
                    if(i==column1 and j==table1):
                        print(i,column1,j,table1,colindex1)
                        flag=True
                        break

        if(flag==False):
            print("Column is not present")
            return

        data=[]
        for t in result:   
            if(operator(conditionlist[1],t[colindex1])):
                data.append(t)

        printme(cols,tables,data)


def joinoncondition(cols,tables,conditionlist):
    print("join on conditions")
    condition=conditionlist[2]
    if(condition=="AND" or condition=="OR"):
        result=crossproductmany(tables)
        print(conditionlist)
        colindex1=-1
        colindex2=-1

        if("." in conditionlist[1]):

            if("=" in conditionlist[1]):
                table1=conditionlist[1].split('=')[0].split(".")[0]
                column1=conditionlist[1].split('=')[0].split(".")[1].strip("<").strip(">")

            elif("<" in conditionlist[1]):
                table1=conditionlist[1].split('<')[0].split(".")[0]
                column1=conditionlist[1].split('<')[0].split(".")[1]

            elif(">" in conditionlist[1]):
                table1=conditionlist[1].split('>')[0].split(".")[0]
                column1=conditionlist[1].split('>')[0].split(".")[1]

            flag=False
            for j in tables:
                print("table is ",j)
                if(flag==False):
                    for i in tabledata[j]:
                        colindex1=colindex1+1
                        print("column is ",i)
                        if(i==column1 and j==table1):
                            print(i,column1,j,table1,colindex1)
                            flag=True
                            break

            if(flag==False):
                print("Column is not present")
                return

        if("." in conditionlist[3]):
            if("=" in conditionlist[3]):
                table2=conditionlist[3].split('=')[0].split(".")[0]
                column2=conditionlist[3].split('=')[0].split(".")[1].strip("<").strip(">")

            elif("<" in conditionlist[3]):
                table2=conditionlist[3].split('<')[0].split(".")[0]
                column2=conditionlist[3].split('<')[0].split(".")[1]

            elif(">" in conditionlist[3]):
                table2=conditionlist[3].split('>')[0].split(".")[0]
                column2=conditionlist[3].split('>')[0].split(".")[1]

            flag=False
            for j in tables:
                print("table is ",j)
                if(flag==False):
                    for i in tabledata[j]:
                        colindex2=colindex2+1
                        print("column is ",i)
                        if(i==column1 and j==table2):
                            print(i,column2,j,table2,colindex2)
                            flag=True
                            break

            if(flag==False):
                print("Column is not present")
                return
        
        
        data=[]
        # print('2',result)
        print(table1,table2)
        print(colindex1,colindex2)
        for t in result:
            #print(t)
            if(condition=="AND"):
                if(operator(conditionlist[1],int(t[colindex1])) and operator(conditionlist[3],int(t[colindex2]))):
                    print(t[colindex1])
                    data.append(t)
            elif(condition=="OR"):
                if(operator(conditionlist[1],t[colindex1]) or operator(conditionlist[3],t[colindex2])):
                    data.append(t)
        
        printme(cols,tables,data)
        
def joinwithtablenameonbothside(cols,tables,conditionlist,msg):
    print("joinwithtablenameonbothside")
    result=crossproductmany(tables)
    
    table1,column1=splitonoperator(conditionlist[1])[0].split(".")
    table2,column2=splitonoperator(conditionlist[1])[1].split(".")
    colindex1=getindex(table1,column1)
    colindex2=getindex(table2,column2)

    print(table1,column1,table2,column2)
    
    if(int(colindex1)==-1 or int(colindex2)==-1):
        print("column not present")
        return
    data=[]
    
    for j in result:
        colnum=-1
        t=[]
       
        if(checkoperator(conditionlist[1],j[colindex1],j[colindex2+len(tabledata[table1])])):
            print(j[colindex1],j[colindex2+len(tabledata[table1])])
            for i in j:
                colnum=colnum+1
                if(colnum!=colindex2+len(tabledata[table1])):
                    t.append(i)
            data.append(t)
    if(msg):
        printme(cols,tables,data)
    else:
        t=[]
        for i in tabledata[table1]:
            t.append(table1+"."+i)
        for i in tabledata[table2]:
            if(i!=column2):
                t.append(table2+"."+i)

        data.append(t)
        return data

def joinoneconditionandtablename(cols,tables,conditionlist):
    print("joinoneconditionandtablename")
    print(conditionlist[0:2])
    data=joinwithtablenameonbothside(cols,tables,conditionlist[0:2],False)

    head=data[len(data)-1]
    data=data[:-1]
    print(head)
    for i in data:
        print(i)

    print(conditionlist[3])
    tabledt,columnval=splitonoperator(conditionlist[3])
    table,columnname=tabledt.split(".")
    print(tabledt,columnval)
    print(table,columnname)
    flag=False

    for i in tabledata[table]:
        if(i==columnname):
            flag=True
            break

    if(flag==False):
        print('Column of second column not present')
        return

    colindex=-1
    for i in head:
        colindex=colindex+1
        if(columnname==i.split(".")[1]):
            break

    print(colindex)

    result=[]
    for i in data:
        if(checkoperator(conditionlist[3],i[colindex],columnval)):
            result.append(i)
    
    printme(cols,tables,result)

def conditionononetable(cols,tables,conditionlist):

    lines=readfile(tables[0]+".csv")
    table1,column1=splitonoperator(conditionlist[1])[0].split(".")
    colindex=getindex(table1,column1)
    print(table1,column1,colindex)
    val=splitonoperator(conditionlist[1])[1]
    data=[]
    for i in lines:
        if(checkoperator(conditionlist[1],i[colindex],val)):
            data.append(i)

    printme(cols,tables,data)

def twoconditionononetable(cols,tables,conditionlist):
    lines=readfile(tables[0]+".csv")
    table1,column1=splitonoperator(conditionlist[1])[0].split(".")
    table2,column2=splitonoperator(conditionlist[3])[0].split(".")

    colindex1=getindex(table1,column1)
    colindex2=getindex(table2,column2)

    print(table1,column1,colindex1)

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

    printme(cols,tables,data)

def processquery(args):
    
    aggregate=args[1].split('(')[0]
    tables=args[3].split(',')

    print(args)
    if(len(args)==4 and args[0]=="select" and args[1]=="*" and len(tables)==1):
        selectall(args)

    elif(len(args)==4 and args[0]=="select" and args[1]=="*" and len(tables)>=2):
        joinmany(tables)

    elif(len(args)==4 and args[0]=="select" and (aggregate=="max" or aggregate=="min" or aggregate=="sum" or aggregate=="average") and len(tables)==1 ):
        selectaggregate(args)

    elif(len(args)==4 and args[0]=="select" and aggregate=="distinct" and len(tables)==1):
        params=args[1].split('(')
        cols=params[1].strip(')').split(',')
        if(len(cols)==1):
            selectdistinct(cols,args[3])
        else:
            print("Please specify one column only")

    elif(len(args)==4 and args[0]=="select" and len(tables)>1):
        params=args[1].split('(')
        cols=params[0].split(',')
        selectmanycolums(cols,tables,True)

    elif(len(args)==4 and args[0]=="select" and len(tables)==1):
        params=args[1].split('(')
        cols=params[0].split(',')
        selectonecolumn(cols,tables[0])

    elif(len(args)==5 and args[0]=="select" and len(tables)>=2):
        params=args[1].split('(')
        cols=params[0].split(',')
        listcond=args[4].split()
        print(len(listcond))
        print(params)
        print('col is ',cols)
        print(listcond)
        if(len(cols)>=1):
            if(len(listcond)==4):
                if("." in splitonoperator(listcond[1])[0] and "." in splitonoperator(listcond[1])[1] and 
                    "." in splitonoperator(listcond[3])[0] and "." in splitonoperator(listcond[3])[1] ):
                    print("TWo cloumns equated")

                elif("." in splitonoperator(listcond[1])[0] and "." in splitonoperator(listcond[1])[1]):
                    joinoneconditionandtablename(cols,tables,listcond)
                else:
                    joinoncondition(cols,tables,listcond)
            elif(len(listcond)==2):
                
                print(splitonoperator(listcond[1]))
                if("." in splitonoperator(listcond[1])[0] and "." in splitonoperator(listcond[1])[1]):
                    print(". on both side")
                    joinwithtablenameonbothside(cols,tables,listcond,True)
                else:
                    joinone(cols,tables,listcond)
                print()
            else:
                print("Please specify proper conditions")
        else:
            print("Please specify at least one column")

    elif(len(args)==5 and args[0]=="select" and len(tables)==1):
        params=args[1].split('(')
        cols=params[0].split(',')
        listcond=args[4].split()
        print(len(listcond))
        print(params)
        print('col is ',cols)
        print(listcond)

        if(len(listcond)==4):
            twoconditionononetable(cols,tables,listcond)

        elif(len(listcond)==2):
            conditionononetable(cols,tables,listcond)

        
input=sys.argv[1]
'''res=sqlparse.parse(input)
print(res)
stmt=res[0]
print(stmt.tokens)'''

t=sqlparse.sql.IdentifierList(sqlparse.parse(input)[0].tokens).get_identifiers()

for i in t:
    args.append(str(i))

readmetadata()
processquery(args)

