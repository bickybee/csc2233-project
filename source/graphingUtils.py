# library
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import matplotlib
from scipy.interpolate import griddata


DIRECTORY = "../data/graph_locality_data"


def retrieve_locality_data(directory_name):
    # Retrieving locality data
    directory = os.fsencode(directory_name)
    data = []
    # data[0] = pd.read_csv("graph_locality_data/workloada_trace_ext4_probability.csv")

    for i,filename in enumerate(os.listdir(directory)):
        filename_string = filename.decode('ASCII')
        if filename_string.endswith(".csv"):
            data.append(pd.read_csv(directory_name + "/" + filename_string)[['Time', 'Distance','Probability']])

    return data

def plot_locality_data(data):

    # plot figures
    fig = plt.figure()

    # set font
    font = {'family': 'normal',
            'size': 12}
    matplotlib.rc('font', **font)

    for i in range(9):
        x1 = np.linspace(data[i]['Distance'].min(), data[i]['Distance'].max(), len(data[i]['Distance'].unique()))
        y1 = np.linspace(data[i]['Time'].min(), data[i]['Time'].max(), len(data[i]['Time'].unique()))
        x2, y2 = np.meshgrid(x1, y1)
        z2 = griddata((data[i]['Distance'], data[i]['Time']), data[i]['Probability'], (x2, y2), method='cubic')

        #plotting the figure
        ax = fig.add_subplot(3, 3, i+1, projection='3d')
        ax = fig.gca(projection='3d')
        ax.w_xaxis.set_pane_color((1.0, 1.0, 1.0, 1.0))
        ax.w_yaxis.set_pane_color((1.0, 1.0, 1.0, 1.0))
        ax.w_zaxis.set_pane_color((1.0, 1.0, 1.0, 1.0))

        if(i<=2):
            ax.plot_surface(x2, y2, z2, color='white', linewidth=0.1, edgecolors='black',vmax=0, vmin=0)
        if(i>2 and i<=5):
            ax.plot_surface(x2, y2, z2, color='white', linewidth=0.1, edgecolors='black')
        if(i>5):
            ax.plot_surface(x2, y2, z2, color='white', linewidth=0.1, edgecolors='black')

        ax.view_init(azim=225)
        ax.set_zlim([0, 1])

        #ax.set_xlabel('Dist. [Pages]')
        #ax.set_ylabel('Time [I/Os]')
        #ax.set_zlabel('Probability $P_D,_T$', rotation=90)
        #ax.zaxis.set_rotate_label(False)

        if(i!=6):
            ax.set_yticklabels([])
            ax.set_xticklabels([])
            ax.set_zticklabels([])
        else:
            ax.set_xlabel('Dist. [Pages]')
            ax.set_ylabel('Time [I/Os]')
            ax.set_zlabel('Probability $P_D,_T$', rotation=90)
            ax.zaxis.set_rotate_label(False)
    plt.show()

if __name__ == '__main__':
    data = retrieve_locality_data(DIRECTORY)
    plot_locality_data(data)
