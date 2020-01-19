import sys
import sqlparse

tabledata={}
tablename=""
args=[]

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
    print(tabledata)

def selectall(args):
    f1=open(args[3]+".csv","r")      
    line=f1.readline()
    temp=""
    
    for i in tabledata[args[3]]:
        temp=temp+args[3]+'.'+i+'\t'
    
    temp=temp[:-1]
    print(temp)
    while line:
        for j in line.strip().split(','):
            print(j,end='\t\t')
        line=f1.readline()
        print('\n')

    f1.close()

def crossproduct(table1,table2):
    f1=open(table1+".csv","r")
    f2=open(table2+".csv","r")

    list1=f1.readlines()
    list2=f2.readlines()
    result=[]
    for i in list1:
        for j in list2:
            y=i.strip().split(',')
            z=j.strip().split(',')
            result.append(y+z)

    f1.close()
    f2.close()

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
        f1=open(args[3]+".csv","r")
        l=f1.readlines()
        
        for i in tabledata[args[3]]:
            colnum=colnum+1
            if i==cols[0]:
                print(args[3]+"."+i)
                flag=True
                break
        
        if(flag==False):
            print('No such column exist')
            return
        data=[ int(i.split(',')[colnum]) for i in l]
        
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
            
        f1.close()
    else:
        print('Enter one column only')      
        
def selectdistinct(t_col,t_table):
    f1=open(t_table+".csv","r")
    l=f1.readlines()
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
    
    data=set([ int(i.split(',')[colnum]) for i in l])
    print(data)
    f1.close()

def selectonecolumn(t_cols,t_table):
    f1=open(t_table+".csv","r")
    l=f1.readlines()
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
            t=t+i.split(',')[colmapping[j]]+"\t\t"
        print(t)

    f1.close()

def selectmanycolums(cols,tables):
    colmapping={}
    t=""
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

    print(t)
    result=crossproductmany(tables)
    for i in result:
        for j in cols:
            print(i[colmapping[j]],end="\t\t")
        print()

def checkoperator(data):
    if("<" in data):
        return "<"
    elif("<=" in data):
        return "<="
    elif(">" in data):
        return ">"
    elif(">=" in data):
        return ">="
    elif("=" in data):
        return "="


def joinwithcondition(cols,tables,conditions):
    

def joincolumns(tables,t_cols,listcond):
    print("Joining columns")
    condition=listcond[2]
    if(condition=="AND" or condition=="OR"):  
        f1=open(tables[0]+".csv","r")
        f2=open(tables[1]+".csv","r")

        list1=f1.readlines()
        list2=f2.readlines()

        operator1=checkoperator(listcond[1])
        operator2=checkoperator(listcond[3])

        cond1=[]
        cond2=[]
        colmapping1={}
        colmapping2={}
        tempdata=[]
        data=[]

        for i in t_cols:
            colnum=-1
            flag=False
            for j in tabledata[tables[0]]:
                colnum=colnum+1
                if(i==j):
                    colmapping1[i]=colnum
                    flag=True
                    break

            colnum=-1
            for j in tabledata[tables[1]]:
                colnum=colnum+1
                if(i==j):
                    colmapping2[i]=colnum
                    flag=True
                    break
            
            if(flag==False):
                print("Column ",i," is not present in any table")
                return
        
        for i in list1:
            
            for j in list2:
                t=[]
                t.append(i.strip().split(','))
                t.append(j.strip().split(','))
            

            
                print(t)
                print(t[colmapping1[listcond[1].split(operator1)[0]]][1])
                tempdata.append(t)

        
        f1.close()
        f2.close()

def processquery(args):
    
    aggregate=args[1].split('(')[0]
    tables=args[3].split(',')
    print(tables)
    print(args,aggregate)

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
        selectmanycolums(cols,tables)

    elif(len(args)==4 and args[0]=="select" and len(tables)==1):
        params=args[1].split('(')
        cols=params[0].split(',')
        selectonecolumn(cols,tables[0])

    elif(len(args)==5 and args[0]=="select" and len(tables)==2):
        params=args[1].split('(')
        cols=params[0].split(',')
        listcond=args[4].split()
        
        if(len(cols)>=1):
            if(len(listcond)==4):
                joincolumns(tables,cols,listcond)
            else:
                print("Please specify proper conditions")
        else:
            print("Please specify at least one column")

input=sys.argv[1]
'''res=sqlparse.parse(input)
print(res)
stmt=res[0]
print(stmt.tokens)'''

t=sqlparse.sql.IdentifierList(sqlparse.parse(input)[0].tokens).get_identifiers()

for i in t:
    args.append(str(i))

print(args)
readmetadata()
processquery(args)
