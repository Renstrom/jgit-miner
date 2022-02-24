import matplotlib.pyplot as plt
import re
import numpy as np
assertions = "C:\\Users\\ander\\Documents\\Skola\\Master\\jgit-mine\\jgit-miner\\result\\assert.txt"
loc = "C:\\Users\\ander\\Documents\\Skola\\Master\\jgit-mine\\jgit-miner\\result\\loc.txt"




def plotLine(repo, xAxis,yAxis,pl):
    pl.plot(xAxis,yAxis,label=str(repo))

def getMaxMin(summary,xMax,yMax,yMin):
    print(summary)
    if( xMax < int(summary[0])):
        xMax = int(summary[0])
    if( yMin > int(summary[3])):
        yMin = int(summary[3])
    if( yMax < int(summary[2])):
        yMax = int(summary[2])
    return xMax, yMin, yMax
    
    
def plotData(repos, data, xAxisLabel, yAxisLabel, title, summary):
    xMax = -1
    xMin = 0
    yMax = -1
    yMin = 99999999999999
    

    for i in range(len(repos)):
        plotLine(repos[i],data[i][0],data[i][1],plt)
        
        xMax, yMin,yMax = getMaxMin(summary[i],xMax, yMax, yMin)
        
        
    plt.xlabel(xAxisLabel)
    plt.ylabel(yAxisLabel)
    plt.legend(loc="upper right")
    plt.title(title)
    plt.show()

def parseDatapoints(points,summary):
    x = []
    y = []
    
    points = points.replace("{","")
    points = points.replace("}","")
    points = points.replace("[","")
    points = points.replace(","," ")
    points = re.split(" ", points)
    for i in range(len(points)):
        if(points[i]==""):
            continue
        if(i%2==0):
            x.append(points[i])
        else:
            y.append(int(points[i])/int(summary))
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


def getData(objects):
    name    = []
    xy      = []
    summary = []
    for data in objects:
        sum = parseImportantInformation(data[2])
        summary.append(sum)
        name.append(parseImportantInformation(data[0]))
        xy.append(parseDatapoints(data[1],sum[0]))

    return [name,xy,summary]
  

def setup(path, name, xAxisLabel, yAxisLabel):
    dataPoints = readFile(path)
    repos, xy, summary = getData(dataPoints)
    plotData(repos,xy,xAxisLabel,yAxisLabel,name,summary)
    
    
setup(assertions,"Assertions","Number of assertions","Occurences of assertions/number of tests")
    
setup(loc,"LOC","Lines of codes"," Occurences of #LOC/number of tests")
