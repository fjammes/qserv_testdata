import os
import sys
import subprocess

#workerList=["qserv-%d"%i for i in range(0,24)]

workerList=['qserv-0','qserv-1','qserv-10','qserv-11','qserv-12','qserv-13','qserv-14']

qservCmd=". /qserv/stack/loadLSST.bash && \
setup mariadb && \
mysql --socket /qserv/data/mysql/mysql.sock --user=root --password=CHANGEME"

qservShowTables=". /qserv/stack/loadLSST.bash && \
setup mariadb && \
mysqlshow --socket /qserv/data/mysql/mysql.sock --user=root --password=CHANGEME --count "





def ReadTablePerChunk(dbName,tableName):

    chunkNodes={}
    chunkNodes_rows={}

    for worker in workerList:

        cmd='kubectl exec %s -c %s -- su qserv -l -c "%s %s;"'%(worker,"wmgr",qservShowTables,dbName)
    #    print(cmd)

        p=subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1)
        res,err=p.communicate()
        exitcode = p.returncode

        for i,l in enumerate(res.split("\n")):
            l=l.strip()
            if not l.startswith("|"): continue
            tmp=[x for x in l.split(" ") if x!="" and x!="|"]
            if tmp[0].lower()=="tables": continue

            tableName=tmp[0]
            if tableName.endswith("1234567890"): continue
            try:
                chunkId=int(tableName.split("_")[-1])
                if not chunkId in chunkNodes: chunkNodes[chunkId]=[]
                if not worker in chunkNodes[chunkId]: chunkNodes[chunkId].append(worker)

                if not tableName in chunkNodes_rows: chunkNodes_rows[tableName]=0
                chunkNodes_rows[tableName]+=int(tmp[-1])

            except:
                pass

    return chunkNodes,chunkNodes_rows


def ReadDistinctValuesFromTable(dbName,tableName,paramName):

    chunkNodes,chunkNodes_rows = ReadTablePerChunk(dbName,None)
    print(chunkNodes)
    
##     qservWorkerList=[]
##     for k in chunkNodes:
##         qservWorkerList+=chunkNodes[k]
##     print(qservWorkerList)

##     qservWorkerList=list(set(qservWorkerList))
##     print(qservWorkerList)


    for key in chunkNodes:

        worker=chunkNodes[key][0]
        request='select distinct %s from %s_%s'%(paramName,tableName,key)
        cmd='kubectl exec %s -c %s -- su qserv -l -c "%s %s -e \'%s\';"'%(worker,"wmgr",qservCmd,dbName,request)
        
        print(cmd)
        p=subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1)
        res,err=p.communicate()
        exitcode = p.returncode
        print(res)
        print(err)
        print(exitcode)

        sys.exit()

    return None


if __name__=="__main__":


    if "--chunk" in sys.argv:
        chunkNodes,chunkNodes_rows = ReadTablePerChunk("qservTest_case170_qserv",None)

        import pprint
        pprint.pprint(chunkNodes)

        totalRows=0
        for key in chunkNodes_rows:
            if not key.startswith("deepCoadd_ref_"): continue
            print(key," : ",chunkNodes_rows[key])
            totalRows+=chunkNodes_rows[key]
        print("# rows : ",totalRows)
        
    if "--distinct_ids" in sys.argv:
        
        valuePerChunkDict = ReadDistinctValuesFromTable("qservTest_case170_qserv","deepCoadd_ref","tract,patch")
        
        
