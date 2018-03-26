import ast
import pandas

def __exec_then_eval(code):
    ## processes statements and expressesions separatrly
    ## returns the value of last expression
    block = ast.parse(code, mode="exec")
    lv = None
    for i in block.body:
        if isinstance(i,ast.Expr):
            lv = eval(compile(ast.Expression(i.value), "<string>", mode="eval"),globals())
        else:
            lv = None
            exec(compile( ast.Interactive([i]), "<string>", mode="single"),globals())
    return lv


def __saveToS3(obj,bucket,s3path,prefix=""):
    clz =  obj.__class__.__name__
    lastobject=None
    if isinstance(obj,pandas.core.frame.DataFrame):
        import feather as ft
        ft.write_dataframe(obj,"/tmp/{}_lastobject.feather".format(prefix))
        lastobject = "{}_lastobject.feather".format(prefix)
    elif clz == "dict":
        import json
        with open("/tmp/{}_lastobject.json".format(prefix), "w") as outfile:
            json.dump(obj, outfile)
            lastobject="{}_lastobject.json".format(prefix)
    if lastobject is not None:
        import boto
        from boto.s3.connection import S3Connection
        from boto.s3.key import Key
        conn = S3Connection()
        c = boto.connect_s3()
        b = c.get_bucket(bucket)
        k = Key(b)
        k.key = "{}/{}".format(s3path,lastobject)
        k.set_contents_from_filename("/tmp/{}".format(lastobject))           
        
def __cps3(bucket, path,infile):
  import boto
  from boto.s3.connection import S3Connection
  conn = S3Connection()
  from boto.s3.key import Key
  c = boto.connect_s3()
  b = c.get_bucket(bucket)
  k = Key(b)
  k.key = path #'sguha/tmp/pydbx-logger.txt'
  k.set_contents_from_filename(infile)

  
def __getStuff(oo):
    from time import gmtime, strftime
    v=sc.statusTracker().getJobIdsForGroup(oo)
    v = filter(lambda x: sc.statusTracker().getJobInfo(x).status=='RUNNING',v)
    if len(v)>0:
        v2=sc.statusTracker().getJobInfo(v[0])
        s=list(v2.stageIds)
    else:
        v2=None
        s=None
    def parseStage(s):
        temp = {'id':0, 'name':'NA', 'ntasks':0, 'natasks':0, 'nctasks':0, 'nftasks':0}
        temp['id'] = s.stageId
        temp['name'] = s.name
        temp['ntasks'] = s.numTasks
        temp['natasks'] = s.numActiveTasks
        temp['nctasks'] = s.numCompletedTasks
        temp['nftasks'] = s.numFailedTasks
        return temp
    if s is not None:
        f = [ parseStage(sc.statusTracker().getStageInfo(a)) for a in s]
        return {'jobid': v[0], 'data':f,'t':strftime('%H:%M:%S', gmtime())}
    else:
        return {'t':strftime('%H:%M:%S UTC', gmtime())}
        
