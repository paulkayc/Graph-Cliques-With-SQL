import psycopg2 as ps
from config import config
import pandas as pd
import sys



class graphCliques(object):

    def __init__(self):
       pass
#Connect to database, execute an SQL Statement and return Result
    def connect(statement):
        sql_insert = "INSERT INTO r"
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = ps.connect(**params)
            
            # create a cursor
            cur = conn.cursor()
            
        # execute a statement
            #print('PostgreSQL database version:')
            #cur.execute('SELECT version()')
            cur.execute(statement)
            rows = cur.fetchall()
            #cur.fetchall()
            #vendor_id = cur.fetchone()[0]
            #print('vendor id: '+vendor_id)

            conn.commit()          
        
        # close the communication with the PostgreSQL
            cur.close()
        except (Exception, ps.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
        return rows

    def sqlStmt(tbl, src, dest, k):
        stmt=list()
        v=list() 
        t=list()
        #stmt.append('INSERT INTO '+'wiki.r'+str(k)+'(v1,v2,v3)' +' SELECT ')
        stmt.append('SELECT ')
        stmtStr=''

        if k == 1:
            stmtStr = str('SELECT DISTINCT '+src +' FROM ' +tbl+ ' ORDER BY '+src)


        if k == 2:
            stmtStr = str('SELECT '+ 't1'+'.' +src+ ' AS '+ src+ ','+ 't2'+'.'+dest+ ' AS '+ dest+ ' FROM '+ tbl+ 
            ' t1 '+ ' JOIN '+ tbl+ ' t2 '+ ' ON '+ 't1'+'.'+dest+ '='+ 't2'+'.'+src+ ' ORDER BY '+ 't1'+'.'+src+';')     

        if k >2:
            
            for i in range(1, k+1):
                v.append('v'+str(i))
                t.append('t'+str(i))

            stmt.append(t[0]+'.'+src+' AS '+v[0]+',')
            stmt.append(t[0]+'.'+dest+' AS '+v[1]+',')

            cma = 2
            for i in range(2, k):
                if cma < k-1:
                    stmt.append(t[i-1]+'.'+dest+' AS '+v[i]+',')
                    cma+=1
                else:
                    stmt.append(t[i-1]+'.'+dest+' AS '+v[i])

            stmt.append('FROM '+tbl+ ' ' +t[0])

            for i in range(0, k-1):
                stmt.append('JOIN ' + tbl +' '+ t[i+1] + ' ON '+t[i]+'.'+dest+ '='+t[i+1]+'.'+src)
            
            stmt.append(' WHERE ')

            count = 1
            for i in range(0, k):
                if count <= k-1:
                    stmt.append(t[i]+'.'+src+'<'+t[i]+'.'+dest+ ' AND ')
                    count+=1
                else:
                    stmt.append(t[i]+'.'+dest+'='+t[0]+'.'+src+ ';')

            stmtStr = ' '.join(str(item) for item in stmt)

        

        print(stmtStr)
        return(stmtStr)


# define main function with default values
def main(tbl, src, dest, k):

            sqlStatement = graphCliques.sqlStmt(tbl, src, dest, k)
            result = graphCliques.connect(sqlStatement)
            #graphCliques.connect(sqlStatement)
            print('------ CLIQUES OF '+str(k)+ '------')
            print(pd.DataFrame(result))


if __name__ == '__main__':

	#Default Values
    tbl="wiki.c"
    src = 'i'
    dest = 'j'
    k = 3
	#####

    try:
        input_str = sys.argv[1]
        input_arguments = input_str.split(";")
        for arg in input_arguments:
            if arg.startswith("tbl"):
                tbl = str(arg.split("=")[1])
            elif arg.startswith("src"):
                src = str(arg.split("=")[1])
            elif arg.startswith("dest"):
                dest = str(arg.split("=")[1])
            elif arg.startswith("k"):
                k = int(arg.split("=")[1])
            else:
                continue
        if len(input_arguments) < 4:
            print("Use default values for unspecified arguements.\n")
    except:
        print("Use default values for unspecified arguements\n")

    print("## Parameters:")
    print(f"table_name: {tbl}")
    print(f"source: {src}")
    print(f"destination: {dest}")
    print(f"k: {k}")

    main(tbl=tbl, src=src, dest=dest, k=k)

