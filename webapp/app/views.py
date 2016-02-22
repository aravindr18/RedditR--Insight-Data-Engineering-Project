from app import app
from flask import jsonify
from flask import request
from flask import render_template, flash, redirect
import datetime
import requests
from bs4 import BeautifulSoup
from cassandra.cluster import Cluster
from cassandra.policies import ConstantReconnectionPolicy
from flask import Flask, make_response, request
import simplejson as json
import random
import os
import operator
from operator import itemgetter
import time
import cPickle
#
#
cluster = Cluster(['172.31.1.44','172.31.1.45','172.31.1.46']) 

session = cluster.connect('flashback') 
session.default_timeout = 60


@app.route('/')
def index():
    return render_template("index.html")


@app.route('/flashBack', methods=['GET'])
def render():
    return render_template('flashback.html')
@app.route('/flashBack', methods=['POST'])
def my_form_post():
    # get response from the form
    text = request.form['text']
    #check = request.form['option']
    checked = 'option' in request.form
    #print checked
    
    if checked:
        stmt = "SELECT * FROM firstpost WHERE author=%s"
    else:
        stmt = "SELECT * FROM firstpost WHERE author=%s LIMIT 1"
    
    response = session.execute(stmt, parameters=[text])
   
    def findLink(link_id):
        id = link_id.split('t3_')[1]
        weblink = "http://www.reddit.com/"+id
        r = requests.get(weblink,headers = {'User-agent': 'webApp'})
        soup=BeautifulSoup(r.text)
        return (soup.title.text.split(":")[0],weblink)
    
    response_list = []
    
    for val in response:
        response_list.append(val)
    
    if len(response_list)==0:
        return render_template("404.html")
    if checked:
        jsonresponse = [{"author": x.author, "posted_at": x.created_utc\
            , "post": x.body,"discussion": x.link_id, "subreddit": x.subreddit} for x in response_list]
    
        response = make_response('\n'.join('{}'.format(json.dumps(x)) for x in jsonresponse) + "\n")
        response.headers["Content-Disposition"] = "attachment; filename=%s_data.json" %text
        return response
    else:
        jsonresponse = [{"author": x.author, "posted_at":
                datetime.datetime.fromtimestamp(x.created_utc).strftime('%m-%d-%Y %H:%M:%S')\
                , "post": x.body,"discussion": findLink(x.link_id), "subreddit": x.subreddit} for x in response_list]
    
        return render_template('flashback_response.html',data=jsonresponse)



@app.route('/404',methods =['GET'])
def errorPage():
    return render_template('404.html')
@app.route('/snapShot', methods =['GET','POST'])
def snapShot():
    
    if request.method=="GET":
        return render_template('snapshot.html')
    if request.method=="POST":
        response_list=[]

        if 'year' in request.form and 'month' in request.form: 
             year = request.form['year']
             month = request.form['month']
        
        
        stmt = "SELECT * FROM subredditinfo WHERE year=%s and month =%s"
        response = session.execute(stmt, parameters=[int(year),int(month)])
        for val in response:
            response_list.append(val)

        if len(response_list)==0:
            return render_template("404.html")
        jsonresponse = [{"subreddit": x.subreddit, "totalComments":x.total_comments\
                , "distinctAuthors": x.distinct_authors,"distinctSubmission": x.distinct_submissions,\
                 "commentsPerDAuthor": x.comments_per_distinct_author, \
                 "commentsPerDSubmission":x.comments_per_distinct_submission,"year":year, "month":month} for x in response_list]
    
        
        
        return render_template('snapshot_viz.html',data=jsonresponse)

@app.route('/engage', methods=['GET','POST'])
def engage():
    subredditList = []
    count=0
    if request.method=="GET":
        
        with open(os.path.join(os.path.dirname(__file__),'subredditlist.txt')) as f:
            for line in f:
                subredditList.append(line.strip())
        #print subredditList
        return render_template('engage.html',data=subredditList)
    
    if request.method =="POST":
        
        response_indegree=[]
        response_outdegree = []
        response_trend=[]
        if 'subr' in request.form:
            subreddit = request.form['subr']

        checked = 'darthvader' in request.form
    
    
        if checked:
            stmt1 = "SELECT * FROM indegree WHERE subreddit=%s"
            stmt2 = "SELECT * FROM outdegree WHERE subreddit=%s"
        else:
            stmt1 = "SELECT * FROM indegree WHERE subreddit=%s limit 1"
            stmt2 = "SELECT * FROM outdegree WHERE subreddit=%s limit 1"
    
    indegree = session.execute(stmt1, parameters=[subreddit])
    outdegree = session.execute(stmt2,parameters=[subreddit])
    stmt3 = "SELECT * FROM subredditinfo WHERE subreddit=%s"
    sorted_trend = session.execute(stmt3, parameters=[subreddit])
    
    for val in sorted_trend:
        response_trend.append(val)
    for val in indegree:
        response_indegree.append(val)
    for val in outdegree:
        response_outdegree.append(val)
    json_indegree=[]
    json_outdegree=[]
    

    #if len(response_trend)==0 or len(response_indegree) ==0 or len(response_outdegree==0):
     #   return render_template("404.html")
    json_response=[{"year":x.year,"month":x.month,"total_comments":x.total_comments,"distinct_authors":x.distinct_authors
                        } for x in response_trend]

    response_trendSorted = sorted(list(json_response), key=itemgetter('year','month'), reverse=False)
    
    if not checked:
        json_indegree = [{"subreddit": x.subreddit, "influence":x.rank, "user": x.author} for x in response_indegree]
        json_outdegree = [{"subreddit": x.subreddit, "influence":x.rank, "user": x.author} for x in response_outdegree]
        # extract user for subreddit recommendation
        
        try:
            user = json_outdegree[0]["user"]
        except:
            return render_template("404.html")
        
        subreddit= json_outdegree[0]["subreddit"]
        recommendation=[]   # list to store the response from cassandra
        query1 = "SELECT subreddit FROM firstpost where author=%s"  #
        #query2 = "SELECT subreddit FROM firstpost where author=%s"
        query1_response = session.execute(query1,parameters=[user])
        #query2_response = session.execute(query2, parameters=[user])

        for val in query1_response:
            recommendation.append(val)
        
        

        #profanity=cPickle.load(open(os.path.join(os.path.dirname(__file__),'profanity_filter.xyz'),'rb'))
        #print profanity
        profanity=[]
        with open(os.path.join(os.path.dirname(__file__),'profanity_filter.txt')) as f:
            for line in f:
                profanity.append(line.strip())
        recommendation_unique= random.sample(set(recommendation),6) # make it unique!
        
        

        for idx, val in enumerate(recommendation_unique):
            if recommendation_unique[idx].subreddit in profanity or subreddit in recommendation_unique:
                print recommendation_unique[idx]
                del recommendation_unique[idx]

        
        # wrap it as JSON

        
        try:
            recommendation_unique.remove(subreddit)
        except:
            pass
        json_recommendation = [{"subreddit":x.subreddit} for x in recommendation_unique]


        return render_template('engage_viz.html',indegree=json_indegree, outdegree=json_outdegree,trend = response_trendSorted, recommend = json_recommendation)

    else:
        json_indegree = [{"subreddit": x.subreddit, "influence":x.rank, "user": x.author} for x in response_indegree]
        json_outdegree = [{"subreddit": x.subreddit, "influence":x.rank, "user": x.author} for x in response_outdegree]
        
        finalList = json_indegree + json_outdegree
        responseIn = make_response('\n'.join('{}'.format(json.dumps(x)) for x in finalList) + "\n **** \n")
        
        responseIn.headers["Content-Disposition"] = "attachment; filename=engagement_data.json"
        
        
        return responseIn


# Real time component


@app.route('/api/minute-trend/')
def minuteTrend():
    
    minuteslot = long(time.strftime('%Y%m%d%H%M',time.localtime(time.time() - 120)))
    stmt = "SELECT * FROM minute_top_trends WHERE minuteslot=%s"
    response = session.execute(stmt,parameters=[long(minuteslot)])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"minuteslot": x.minuteslot, "subreddit": x.subreddit, "count": x.count} for x in response_list]
    sortedJson = sorted(list(jsonresponse), key=itemgetter('count'), reverse=False)[:20]

    return jsonify(trends=sortedJson)

@app.route('/api/user-trend/')
def userTrend():
    
    minuteslot = long(time.strftime('%Y%m%d%H%M',time.localtime(time.time() - 120)))
    stmt = "SELECT * FROM minute_top_authors WHERE minuteslot=%s"
    response = session.execute(stmt,parameters=[long(minuteslot)])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"minuteslot": x.minuteslot, "author": x.author, "count": x.count} for x in response_list]
    sortedJson = sorted(list(jsonresponse), key=itemgetter('count'), reverse=False)[:20]

    return jsonify(user_trend=sortedJson)


@app.route('/api/rt/')
def rt():
    
    secslot = long((time.time()) - 60)
    stmt = "SELECT * FROM rt_reddit WHERE secslot=%s limit 20"
    response = session.execute(stmt, parameters=[secslot])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"subreddit": x.subreddit, "author": x.author, "time_ms": x.created_utc, "body": x.body} for x in response_list]
    
    return jsonify(rt=jsonresponse)

@app.route('/trends', methods=['GET','POST'])
def trends():
    if request.method =="GET":
        return render_template("trends.html")





if __name__ == '__main__':
  app.run(debug=True)