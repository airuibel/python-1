# -*-
import string
import matplotlib.pyplot as plt
import numpy as np

if __name__ == '__main__':
    file = open("data2.csv", 'r')
    linesList = file.readlines()
    #     print(linesList)
    linesList = [line.strip().split(",") for line in linesList]
    file.close()
    print("linesList:")
    print(linesList)
    years = [x[0] for x in linesList]
    print(years)
    price = [x[1] for x in linesList]
    print(price)
    plt.plot(years, price, 'b.')  # ,label=$cos(x^2)$)
    # plt.plot(years, price, 'r')
    plt.xlabel("id")
    plt.ylabel("rank")
    # plt.ylim(0, 15)
    plt.title('distrubtion')
    plt.legend()
    for i in range(0, 901, 100):
        plt.axvline(i)
    for k in range(0, 901, 100):
        plt.axhline(k)
    plt.axvline(120, color='r')
    plt.show()
