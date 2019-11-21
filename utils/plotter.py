#!/usr/bin/python3

import sys
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as mticker
import ntpath
import os

output_path = ""
filename = ""

if sys.argv[1] == 'cluster': # kMeans und random clustering
    for input in range(2, len(sys.argv)):
        inputFile = open(sys.argv[input], "r")
        xData = []
        yData = []
        for line in inputFile:
            dataPoint = line.split()
            if len(dataPoint) == 2:
                xData.append(int(dataPoint[0]))
                yData.append(float(dataPoint[1]))
        plt.plot(xData, yData, label=ntpath.basename(sys.argv[input]).strip('.txt'))
    plt.grid(True)
    plt.xlabel('k')
    plt.ylabel('cost')
    plt.title('Clustering')
    plt.legend()
    plt.xticks(np.arange(0, 21, 2))
    f = mticker.ScalarFormatter(useOffset=False, useMathText=True)
    g = lambda x,pos : "${}$".format(f._formatSciNotation('%1.10e' % x))
    plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(g))
    output_path = os.path.dirname(sys.argv[2]) + "/"
    filename = "graph"

elif sys.argv[1] == 'als': # latentes variablenmodell
    for input in range(2, len(sys.argv)):
        inputFile = open(sys.argv[input], "r")
        xData = []
        yData = []
        for line in inputFile:
            dataPoint = line.split()
            if len(dataPoint) == 2:
                xData.append(int(dataPoint[0]))
                yData.append(float(dataPoint[1]))
        plt.plot(xData, yData, label='lambda = ' + ntpath.basename(sys.argv[input]).strip('.txt').split('_')[1])
    plt.grid(True)
    plt.xlabel('Iterations')
    plt.ylabel('RMSE')
    plt.title('ALS (Rank = ' + ntpath.basename(sys.argv[input]).strip('.txt').split('_')[0] + ')')
    plt.legend()
    output_path = os.path.dirname(sys.argv[2]) + "/"
    filename = "Rank=" + ntpath.basename(sys.argv[2]).strip('.txt').split('_')[0]

elif sys.argv[1] == 'cf': # collaborative filtering
    inputFile = open(sys.argv[2], "r")
    xData = []
    yData = []
    for line in inputFile:
        dataPoint = line.split()
        if len(dataPoint) == 2:
            xData.append(int(dataPoint[0]))
            yData.append(float(dataPoint[1]))
    plt.plot(xData, yData)
    plt.grid(True)
    plt.xlabel('k')
    plt.ylabel('RMSE')
    plt.title('Collaborative Filtering')
    output_path = os.path.dirname(sys.argv[2]) + "/"
    filename = 'graph'

plt.savefig(output_path + filename + ".png")
