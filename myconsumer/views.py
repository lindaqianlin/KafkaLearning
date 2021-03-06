import pymongo as pm
import json
from django.shortcuts import render

#Connect with MongoDB
myclient = pm.MongoClient('mongodb://localhost:27017/')
mydb = myclient['mydatabase']
myhit = mydb['myhit']

# Create your views here.
def display(request):
    #display from October to December
    cursor = myhit.aggregate([{'$project': {'account':1, '_id':0, 'visit_time':1}},{'$match': {'visit_time': {'$gt':'2020/10/01', '$lt':'2020/12/31'}}},{'$sort':{'visit_time':-1}}])
    f = open('templates/myconsumer/display.html', 'w')
    f.write("<p>Display Active Record</p>\n")
    f.write('<table border="1">\n')
    f.write("  <tr>\n")
    f.write("    <th>Account</th>\n")
    f.write("    <th>Visit Time</th>\n")
    f.write("  </tr>\n")

    for doc in cursor:
        f.write("  <tr>\n")
        for i in doc:
          f.write("    <td>")
          f.write(str(doc[i]))
          f.write("    </td>\n")
        f.write("  </tr>\n")
    f.write("</table>\n")
    f.close()
    return render(request, 'myconsumer/display.html')
