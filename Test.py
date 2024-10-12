import numpy as np

arr = np.array(([1,2,9],[3,7,8],[5,6,7],[6,5,2]))
print(arr.shape)

print(arr.ndim)

print(arr.dtype)

print(arr.size)

print(type(arr))

x=[1,8,0,4,67]

length = len(x)

print("Length of array is ", length)

for n in range(0,length):
    largeNum = x[n]
    if x[n]>largeNum:
        largeNum = x[n]
print(largeNum)