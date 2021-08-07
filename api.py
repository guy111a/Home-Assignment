
import flask
from flask import request, jsonify
import time
# from pyspark import SparkContext
from pyspark import SparkConf, SparkContext
from pyspark import SQLContext

sc = SparkContext(conf=SparkConf())
sqlContext = SQLContext(sc)

# class model(object):
#     @staticmethod
#     def transformation_function(a_model):
#         delim = a_model.delim
#         def _transformation_function(row):
#             return row.split(delim)
#         return _transformation_function

#     def __init__(self):
#         self.delim = ','
#         self.requests = sc.textFile('requests.csv')
#         self.clicks = sc.textFile('clicks.csv')
#         self.impressions = sc.textFile('impressions.csv')
        
#     def run_model(self, uid):
#         self.requests = self.requests.map(model.transformation_function(self)).filter(lambda x: uid  in x)
#         self.clicks = self.clicks.map(model.transformation_function(self)) #.filter(lambda x: uid  in x)
#         self.rdd_join = self.requests.join(self.clicks.map(lambda x: x.split()))
#         #manager_df = manager.map(lambda x: list(x.split(','))).toDF(["col1","col2"])
#         print(f'lookup: {self.requests.collect()}')
#         print(f'join: {self.rdd_join.collect()}')
  
# test = model()
# test.run_model("79c13312-ee60-4ce7-bac1-b03e42e62e8b")
# test.requests.take(10)


requests = sc.textFile('requests.csv')
impressions = sc.textFile('impressions.csv')
clicks = sc.textFile('clicks.csv')

app = flask.Flask(__name__)
app.config["DEBUG"] = True

#  default api reply
@app.route('/', methods = ['GET'])
def home():
    return "<h1>A.P.I</h1><p>Access Point Interface.</p>"


# retuens the current server time
@app.route('/keepalive', methods = ['GET'])
def translate():
    rply = { "system_status" : "ready",
            "current_time" : time.time()}
    print(rply)
    return rply


'''
userStats:
Input: user_id
Output:
Num of requests
Num of impressions
Num of clikcs
Average price for bid (include only wins)
Median Impression duration
Max time passed till click
'''

@app.route('/userStats', methods = ['GET'])
def userStat():
    err = "Usage: IP_ADDRESS | URL:8089/userStats?uid=user id"
    if 'uid' in request.args:
        if str(request.args['uid']) != "" :  
            userID = str(request.args['uid'])
            
            data_rdd = requests.flatMap(lambda line: line.split()).filter(lambda x: userID  in x)

            df_requests = data_rdd.map(lambda x: list(x.split(','))).toDF(["timeStamp","sessionID","partnerName","userID","bid","win"])
            df_clicks = clicks.map(lambda x: list(x.split(','))).toDF(["timeStamp","sessionID","duration"])
            df_impressions = impressions.map(lambda x: list(x.split(','))).toDF(["timeStamp","sessionID","timePassed"])
            
            answers = [] 
            max_bid = 0
            
            for x in data_rdd.collect():
                answers.append(x)
                if float(x.split(",")[4]) > max_bid:
                    max_bid = float(x.split(",")[4])
                print(f'{x}')
            print(f'max bid: {max_bid}')
                
            df_counting_impressions = df_requests.join(df_impressions, df_impressions.sessionID \
                == df_requests.sessionID, 'left').select(df_requests.sessionID, \
                df_impressions.sessionID).collect()
                
            df_counting_clicks = df_requests.join(df_clicks, df_clicks.sessionID \
                == df_requests.sessionID, 'left').select(df_requests.sessionID, \
                df_clicks.sessionID).collect()
            
            rply = { "user id" : userID, \
                        "session id": df_requests.collect()[0][1], \
                        "number of requests" : len(answers),\
                        "number of impressions": len(df_counting_impressions), \
                        "number of clicks" : len(df_counting_clicks) , \
                        "longest impression": df_impressions.agg({'timePassed': 'max'}).collect(), \
                        "avg price of bid": df_requests.agg({'bid': 'avg'}).collect(), \
                        "max bid price" : max_bid , \
                        "median impression duration": 1    }
            return rply   
        else:
            return err
    else:
        return err



'''
sessionId:
Input: session_id
Output:
Begin: request timestamp
Finish: latest timestamp (request/click/impression)
Partner name
'''

@app.route('/sessionID', methods = ['GET'])
def sessionId():
    err = "Usage: IP_ADDRESS | URL:8089/sessionID?sid=session id"
    if 'sid' in request.args:
        if str(request.args['sid']) != "" :  
            userID = str(request.args['sid'])
                        
            data_rdd = requests.flatMap(lambda line: line.split()).filter(lambda x: userID  in x)

            df_requests = data_rdd.map(lambda x: list(x.split(','))).toDF(["timeStamp","sessionID","partnerName","userID","bid","win"])
            df_clicks = clicks.map(lambda x: list(x.split(','))).toDF(["timeStamp","sessionID","duration"])
            df_impressions = impressions.map(lambda x: list(x.split(','))).toDF(["timeStamp","sessionID","timePassed"])
            # df = df_requests.join(df_impressions, df_impressions.sessionID \
            #         == df_requests.sessionID, 'left').select(df_requests.sessionID, \
            #             df_impressions.timePassed)\
            #         .join(df_clicks, df_clicks.sessionID \
            #         == df_requests.sessionID, 'left').select(df_requests.sessionID, \
            #             df_clicks.duration).collect()
            
            
            df_requests.printSchema()
        
            df_click = df_requests.join(df_clicks, df_clicks.sessionID \
                    == df_requests.sessionID, 'left').select(df_requests.sessionID, \
                        df_clicks.duration).collect()
            clicked = df_click[0][1]
        
            df_impress = df_requests.join(df_impressions, df_impressions.sessionID \
                == df_requests.sessionID, 'left').select(df_requests.sessionID, \
                    df_impressions.timePassed).collect()
            impressed = df_impress[0][1]
            
            rply = { \
                    "begin": str(df_requests.collect()[0][0]),\
                        "clicked" : clicked, \
                        "impressed" : impressed, \
                        "partner name": str(df_requests.collect()[0][2]) }  # sorted(x.join(y).collect())
            return rply   
        else:
            return err
    else:
        return err



app.run(host='0.0.0.0', port='8089')
