# -*- coding : utf-8 -*-
from statistics import mean
import numpy as np
from matplotlib import style
import matplotlib.pyplot as plt

xs = [1, 2, 3, 4, 5]
ys = [5, 4, 6, 5, 6]

xs = np.array(xs, dtype=np.float64)
ys = np.array(ys, dtype=np.float64)


def best_fit_slope_and_intercept(xs, ys):
    m = (((mean(xs) * mean(ys)) - mean(xs * ys)) /
         ((mean(xs) ** 2) - mean(xs ** 2)))
    b = mean(ys) - m * mean(xs)
    return m, b


m, b = best_fit_slope_and_intercept(xs, ys)
regression_line = []
for x in xs:
    regression_line.append((m * x) + b)

predict_x = 7
predict_y = (m * predict_x) + b
predict_x = np.array(predict_x, dtype=np.float64)

style.use('ggplot')
plt.scatter(xs, ys, color='#003F72', label='data')
plt.plot(xs, regression_line, label='regression line')
plt.scatter(predict_x, predict_y, color='#00FF72', label = 'prediction')
plt.legend(loc = 4)
plt.show()
