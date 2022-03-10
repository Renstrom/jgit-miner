import datetime
from time import strptime
from matplotlib import dates
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import re
import numpy as np
from pathlib import Path
import os
from operator import itemgetter
import operator

data_folder = os.path.realpath(os.path.join(os.path.dirname(__file__),'..','..','..','..' ,'jgit-miner','result'))

assertions = data_folder+"/assert.txt"
loc = data_folder+"/loc.txt"
cyclomatic_complexity=data_folder+"/cyclomatic.txt"
time_line_data = data_folder+"/commit.txt"

def plotLine(repo, xAxis,yAxis,pl,option):
    if(option== 0):
        pl.plot(xAxis,yAxis,label=str(repo))
    else :
        xAxis = [datetime.datetime.strptime(d, "%d/%m/%Y").date() for d in xAxis]
        ax = plt.gca()

        formatter = dates.DateFormatter("%Y-%m")
        ax.xaxis.set_major_formatter(formatter)

        locator = mdates.YearLocator()
        ax.xaxis.set_major_locator(locator)
        pl.plot(xAxis,yAxis,label=str(repo))
        plt.xlabel("Date removed(yyyy-mm-dd)")
        plt.xticks(rotation=45)
        plt.ylabel("Number of removed test functions")
        plt.legend(loc="upper right")
        plt.title("Timeline of removed test functions")
        
        plt.show()
        
        


def getMaxMin(summary,xMax,yMax,yMin):
    if( xMax < int(summary[0])):
        xMax = int(summary[0])
    if( yMin > int(summary[3])):
        yMin = int(summary[3])
    if( yMax < int(summary[2])):
        yMax = int(summary[2])
    return xMax, yMin, yMax
    
    
def plotData(repos, data, xAxisLabel, yAxisLabel, title, summary,option):
    xMax = -1
    xMin = 0
    yMax = -1
    yMin = 99999999999999
    top5points = []

    for i in range(len(repos)):
        
        
        if(option == 0):
            xMax, yMin,yMax = getMaxMin(summary[i],xMax, yMax, yMin)
        else :
            data[i] = [[data[i][j][k] for j in range(len(data[i]))] for k in range(len(data[i][0]))]
            data[i] = sorted(data[i],key=lambda x: datetime.datetime.strptime(x[0],'%d/%m/%Y'))
            data[i] = [[data[i][j][k] for j in range(len(data[i]))] for k in range(len(data[i][0]))]

        plotLine(repos[i],data[i][0],data[i][1],plt,option)
        
        
    
    if (option== 0):
        plt.xlabel(xAxisLabel)
        plt.ylabel(yAxisLabel)
        plt.legend(loc="upper right")
        plt.title(title)
        plt.show()


def saveTopPoints(topPoints,name,foo_name):
    f = open("result/topPoints"+"_"+foo_name+".txt", "a")
    f.write(name[0]+" ")
    for point in topPoints:
        point = [str(x) for x in point]
        f.write(','.join(point)+" | ")
    f.write('\n')
    f.close()

def parseDatapoints(points,summary,option,name,foo_name):
    x = []
    y = []
    z = []
    points = points.replace("{","")
    points = points.replace("}"," ")
    points = points.replace("[","")
    points = re.split(" ", points)

    for i in points:
        xy = re.split(",", i)
        if(xy[0]=="" or xy[0] =="-1"):
            continue

        if(option ==0):
            x.append(int(xy[0]))
            y.append(int(xy[1])/int(summary))
            data = [x,y]
        else:
            x.append(xy[0]) #dates
            z.append(xy[1]) #id
            y.append(int(xy[2])) #number
            
           
            data = [x,z,y]
         
    
    transposed_data = [[data[j][k] for j in range(len(data))] for k in range(len(data[0]))]
    
    top_5_points = sorted(transposed_data,key=lambda x: x[-1])[-6:-1]
    saveTopPoints(top_5_points,name,foo_name)
    

    return [x,y]

def parseImportantInformation(points):
    points = points.replace("[","")
    return re.split(" ", points)



def readFile(data):
    datapoints = []
    with open(data) as f:
        lines = f.readlines()
        for line in lines:
            datapoints.append(re.split("]",line))
     
    return datapoints   


def getData(objects,option,foo_name):
    name    = []
    xy      = []
    summary = []

    for data in objects:
        sum = parseImportantInformation(data[2])
        summary.append(sum)
        name.append(parseImportantInformation(data[0]))

        xy.append(parseDatapoints(data[1],sum[0],option,name[-1],foo_name))

    return [name,xy,summary]
  

def setup(path, name, xAxisLabel, yAxisLabel,foo_name):
    dataPoints = readFile(path)
    repos, xy, summary = getData(dataPoints,0,foo_name)
    plotData(repos,xy,xAxisLabel,yAxisLabel,name,summary,0)
    

def setup_time_line(path,name,xAxisLabel,yAxisLabel,foo_name):
    dataPoints=readFile(path)
    name = []
    xy = []
    summary = []
    for data in dataPoints:
        name.append(parseImportantInformation(data[0]))
        xy.append(parseDatapoints(data[1],summary,1,name[-1],foo_name))
    plotData(name,xy,xAxisLabel,yAxisLabel, name,summary,1)


setup(assertions,"Assertions","Number of assertions","Occurences of assertions/number of tests","top_occurences_of_assertions")
    
setup(loc,"Lines of Code","Lines of code"," Occurences of #LOC/number of tests", "top_occurences_numbers_of_tests")
setup(cyclomatic_complexity,"Cyclomatic Complexity","Cyclomatic Complexity","Cyclomatic Complexity/Total Cyclomatic complexity", "top_occurences_cyclomatic_complexity")
setup_time_line(time_line_data,"Timeline of removed test functions", "Date removed(yyyy-mm-dd)", "Number of removed test functions", "top_removed_tests")




