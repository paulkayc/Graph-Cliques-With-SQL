import sys
import vertica_python as vp
from vertica_python.vertica.messages.frontend_messages import password
import time

class graphCliques(object):

    def __init__(self):
       pass

    # Establish Database Connection
    def conn():
        """ Connect to the vertica database server """
        global db
        global cur
        db = vp.connect(database = "cosc6339", user="cs36", password="nn5vJxaZ")
        cur = db.cursor()

    # Generate SQL Statement
    def sqlStmt(tbl, src, dest, k):
        stmt=list()
        v=list() 
        t=list()

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
        return(stmtStr)

def main(tbl, src, dest, k):
            # Generate SQL statement
            sqlStatement = graphCliques.sqlStmt(tbl, src, dest, k)
            sql = 'INSERT INTO ' 'r'f'{k}' ' ' f'{sqlStatement}'

            # Connect to Vertica Database
            graphCliques.conn()

            # Delete records in the result table
            sqldelete = 'DELETE FROM ' 'r'f'{k}' ';'
            cur.execute(sqldelete)
            # commit the changes to the database
            db.commit()

            start_time = time.time()
            # Execute SQL statement
            cur.execute(sql)
            # commit the changes to the database
            db.commit()
            # close communication with the database
            cur.close()
            end_time = time.time()
            execution_time = end_time - start_time
            print('Execution Time is: ' f'{execution_time}')
            print('CLIQUES OF '+str(k)+ ' SAVED TO TABLE k' f'{k}')
            
if __name__ == '__main__':

	# Default Values
    tbl="wikiVote"
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

