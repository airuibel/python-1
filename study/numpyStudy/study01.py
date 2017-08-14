# -*- utf-8 -*-
'''
moves = ['up', 'left', 'down', 'right', 'up', 'left', 'down']


def move_up(x):  # 定义向上的操作
    x[1] += 1


def move_down(x):  # 定义向下的操作
    x[1] -= 1


def move_left(x):  # 定义向左的操作
    x[0] -= 1


def move_right(x):  # 定义向右的操作
    x[0] += 1


# 动作和执行的函数关联起来，函数作为键对应的值
actions = {
    'up': move_up,
    'down': move_down,
    'left': move_left,
    'right': move_right
}

coord = [0, 0]

for move in moves:
    actions[move](coord)
    print(coord)


def get_val_at_pos_1(x):
    return x[1]


heros = [
    ('Superman', 99),
    ('Batman', 100),
    ('Joker', 85)
]

sorted_pairs0 = sorted(heros, key = get_val_at_pos_1)
sorted_pairs1 = sorted(heros, key = lambda x: x[0])

print(sorted_pairs0)
print(sorted_pairs1)

some_ops = lambda x, y: x + y + x * y + x ** y
print(some_ops(2, 3))


# 从10倒数到0
def countdown(x):
    while x >= 0:
        yield x
        x -= 1


for i in countdown(10):
    print(i)


# 打印小于100的斐波那契数
def fibonacci(n):
    a = 0
    b = 1
    while b < n:
        yield b
        a, b = b, a + b


for x in fibonacci(100):
    print(x)

a = [1, 2, 3, 4]
b = [1]
c = [1]
d = b
e = [1, "hello world!", c, False]

print(id(b), id(c))
print(id(b), id(d))
print(b == c)
f = list("abcd")
print(f)
g = [0] * 3 + [1] * 4 + [2] * 2
print(g)
a.pop()
a.append(5)
a.index(2)
a[2]
a += [4, 3, 2]
a.insert(1, 0)
a.remove(2)
a.reverse()
a[3] = 9
b = a[2:5]
c = a[2:-2]
d = a[2:]
e = a[:5]
f = a[:]
a[2: -2] = [1, 2, 3]
g = a[::-1]
a.sort()
print(a)


import random

a = list(range(10))
print(a)
random.shuffle(a)
print(a)
b = sorted(a)
print(b)
c = sorted(a, reverse = True)
print(c)

a = (1, 2)
b = tuple(['3', 4])
c = (5,)
print(c)
d = (6)
e = 3, 4, 5
print(e)


a = ['This', 'is', 'a', 'list', '!']
b = ['This', 'is', 'a', 'tuple', '!']
c = {'This': 'is', 'an': 'unsorted', 'dict': '!'}
for x in a:
    print(x)

for x in b:
    print(x)

for key in c:
    print(key)

for i in range(10):
    print(i)

names = ["suncy", "zhulu", "sunsq"]

for i, name in enumerate(names):
    print(i, name)



pets = ['dog', 'cat', 'droid', 'fly', 'frog']

for pet in pets:
    if pet == 'dog':  # 狗粮
        food = 'steak'  # 牛排
    elif pet == 'cat':  # 猫粮
        food = 'milk'  # 牛奶
    elif pet == 'droid':  # 机器人
        food = 'oil'  # 机油
    elif pet == 'fly':  # 苍蝇
        food = 'sh*t'  #
    else:
        pass
    print(food)

food_for_pet = {
    'dog': 'steak',
    'cat': 'milk',
    'droid': 'oil',
    'fly': 'sh*t'
}

for pet in pets:
    food = food_for_pet[pet] if pet in food_for_pet else None
    print(food)


def traverse_args(*args):
    for arg in args:
        print(arg)


traverse_args(1, 2, 3, 4)


def traverse_kargs(**kwargs):
    for k, v in kwargs.items():
        print(k, v)


traverse_kargs(x = 2, y = 4, z = 5)


def foo(x, y, *args, **kwargs):
    print(x, y)
    print(args)
    print(kwargs)


foo(1, 2, 3, 4, 5, a = 6, b = 'bar')

some_ops = lambda x, y: print(x + y + x * y + x ** y)
some_ops(2, 3)


def another_fibonacci(n):
    a = 0
    b = 1
    while b < n:
        yield b
        a, b = b, a + b

    return "no more ..."


for x in another_fibonacci(3):
    print(x)

class A:
    """class A"""

    def __init__(self, x, y, name):
        self.x = x
        self.y = y
        self._name = name

    def introduce(self):
        print(self._name)

    def greeting(self):
        print("what's up!")

    def __12norm(self):
        return self.x ** 2 + self.y ** 2

    def cal_12norm(self):
        return self.__12norm()


class B(A):
    """Class B inheritenced from A"""

    def greeting(self):
        print("How's going!")

b = B(12, 12, 'zhulu')
b.introduce()
b.greeting()
print(b._name)
print(b.cal_12norm())


m1 = map(lambda x: x ** 2, [1, 2, 3, 4])
m2 = map(lambda x, y: x + y, [1, 2, 3], [5, 6, 7])

# r1 = reduce(lambda x, y: x + y, [1, 2, 3, 4])

f1 = filter(lambda x: x % 2, range(2))
for f in f1:
    print(f)


template = '{name} is {age} years old.'
c = template.format(name='Tom', age=8) # Tom is 8 years old.
d = template.format(age=7, name='Jerry')# Jerry is 7 years old.
print(c, d)




with open('name_age.txt', 'r', encoding = 'utf-8') as f:
    lines = f.readlines()
    for line in lines:
        name, age = line.rstrip().split(',')
        print('{} is {} years old'.format(name, age))

with open('name_age.txt', 'r', encoding = 'utf-8') as fread, open('age_name.txt', 'w', encoding = 'utf-8') as fwrite:
    line = fread.readline()
    while line:
        name, age = line.rstrip().rsplit(',')
        fwrite.write('{},{}\n'.format(age, name))
        line = fread.readline()



import pickle
import os, sys

lines = [
    "I'm like a dog chasing cars.",
    "I wouldn't know what to do if I caught one...",
    "I'd just do things."
]

with open('line.pkl', 'wb') as f:
    pickle.dump(lines, f)

with open('line.pkl', 'rb') as f:
    line_back = pickle.load(f)

print(line_back)

filelist = os.listdir()
for filepath in filelist:  # filelist中是文件路径的列表
    try:
        with open(filepath, 'r') as f:
            # 执行数据处理的相关工作
            ...

        print('{} is processed!'.format(filepath))
    except IOError:
        print('{} with IOError!'.format(filepath))



import os, sys
from multiprocessing import Process

def process_data(filelist):
    for filepath in filelist:
        print('Processing {} ...'.format(filepath))


if __name__ == '__main__':

    full_list = os.listdir()
    n_total = len(full_list)
    n_processes = 4

    length = float(n_total) / float(n_processes)

    indices = [int(round(i * length)) for i in range(n_processes + 1)]

    sublists = [full_list[indices[1]: indices[i + 1]] for i in range(n_processes)]

    processes = [Process(target = process_data, args=(x,)) for x in sublists]

    for p in processes:
        p.start()

    for p in processes:
        p.join()

'''

import os

label_map = {
    'cat': 0,
    'dog': 1,
    'bat': 2
}
with open('data.txt', 'w') as f:
    for root, dirs, files in os.walk('data'):
        for filename in files:
            filepath = os.sep.join([root, filename])
            dirname = root.split(os.sep)[-1]
            label = label_map[dirname]
            line = '{},{}\n'.format(filepath, label)
            f.write(line)



import os, shutil

filepath0 = 'data/bat/IMG_000001.jpg'
filepath1 = 'data/bat/IMG_000000.jpg'

# 修改文件名
os.system('mv {} {}'.format(filepath0, filepath1))
#os.rename(filepath0, filepath1)


# 创建文件夹
dirname = 'data_samples'
os.system('mkdir -p {}'.format(dirname))
#if not os.path.exists(dirname):
#    os.mkdir(dirname)

# 拷贝文件
os.system('cp {} {}'.format(filepath1, dirname))



print("sdsdsds")
