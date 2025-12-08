import math
from itertools import product

import numpy as np
"""a = np.random.randint(1, 10, 10)
A = a > 3
B = a < 8
C = A & B
a[C] = a[C] * -1
print(a)

random_array = np.random.randint(1, 10, 10)
max_index = np.argmax(random_array)
random_array[max_index] = 
print(random_array)

first_array = np.array([[1, 2, 3], [4, 5, 6]])
a = np.outer(first_array, first_array)
print(a)

arr1 = np.random.randint(0, 10, size=(8,3))
arr2 = np.random.randint(0, 10, size=(2,2))
mask = np.isin(arr1, arr2)
result = arr1[mask]
print (arr1)
print(arr2)
print(result)

a = np.random.randint(0, 10, size=(10, 3))
print(a)
mask_1 = (a[:, 0] == a[:, 1])
mask_2 = (a[:, 1] == a[:, 2])
final_mask = mask_1 & mask_2
result = a[final_mask]
print(result)


a = np.random.randint(0, 10, size=(10, 3))
unique_rows = np.unique(a, axis=0)
print(unique_rows)

a = np.array([[1, 0, 1], [2, 0, 2], [3, 0, 3], [4, 4, 4]])
result = np.prod(np.diag(a)[np.diag(a) != 0])
print(result)

import math
a = [[1, 0, 1],
     [2, 0, 2],
     [3, 0, 3],
     [4, 4, 4]]
print(math.prod([a[i][i] for i in range(min(len(a), len(a[0]))) if a[i][i] != 0]))


x = np.array([1, 2, 2, 4])
y = np.array([4, 2, 1, 2])
x_sorted = np.sort(x)
y_sorted = np.sort(y)
a = np.array_equal(x_sorted, y_sorted)
print(a)

a = [1, 2, 2, 4]
b = [4, 2, 1, 2]
a_sorted = a.sort()
b_sorted = b.sort()
print(a_sorted == b_sorted)

x = np.array([6, 2, 0, 3, 0, 0, 5, 7, 0])
mask = x[: -1] == 0
a = x[1:][mask]
max_a = np.max(a)
print(max_a)

x = [6, 2, 0, 3, 0, 0, 5, 7, 0]
max_num = None
for i in range(1, len(x)):
    if x[i - 1] == 0:
        a = x[i]
        if max_num is None:
            max_num = a
        elif a > max_num:
            max_num = a
print(max_num)

x = np.array([2, 2, 2, 3, 3, 3, 5])
a = np.unique(x, return_counts=True)
print(a)

x = [2, 2, 2, 3, 3, 3, 5]
unique_nums = []
count_nums = []
for i in x:
    if i not in unique_nums:
        unique_nums.append(i)
print(unique_nums)
for i in unique_nums:
    counter = 0
    for a in x:
        if a == i:
            counter += 1
    count_nums.append(counter)
print(count_nums)"""

