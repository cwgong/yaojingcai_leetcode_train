"""
给定一个非负整数num。对于0 ≤ i ≤ num 范围中的每个数字i，计算其二进制数中的 1 的数目并将它们作为数组返回。

示例 1:

输入: 2
输出: [0,1,1]
示例2:

输入: 5
输出: [0,1,1,2,1,2]

******************************************************

给定一个排序数组和一个目标值，在数组中找到目标值，并返回其索引。如果目标值不存在于数组中，返回它将会被按顺序插入的位置。

你可以假设数组中无重复元素。

示例 1:

输入: [1,3,5,6], 5
输出: 2
示例2:

输入: [1,3,5,6], 2
输出: 1

*************************************************************

N 辆车沿着一条车道驶向位于target英里之外的共同目的地。

每辆车i以恒定的速度speed[i]（英里/小时），从初始位置position[i]（英里） 沿车道驶向目的地。

一辆车永远不会超过前面的另一辆车，但它可以追上去，并与前车以相同的速度紧接着行驶。

此时，我们会忽略这两辆车之间的距离，也就是说，它们被假定处于相同的位置。

车队是一些由行驶在相同位置、具有相同速度的车组成的非空集合。注意，一辆车也可以是一个车队。

即便一辆车在目的地才赶上了一个车队，它们仍然会被视作是同一个车队。


会有多少车队到达目的地?


示例：

输入：target = 12, position = [10,8,0,5,3], speed = [2,4,1,1,3]
输出：3
解释：
从 10 和 8 开始的车会组成一个车队，它们在 12 处相遇。
从 0 处开始的车无法追上其它车，所以它自己就是一个车队。
从 5 和 3 开始的车会组成一个车队，它们在 6 处相遇。
请注意，在到达目的地之前没有其它车会遇到这些车队，所以答案是 3。

1,5,9

"""



# def bit_num(num):
#     for i in range(num):
#         pass


def shuffle_Z(rownums,ori_str):
    flag = -1
    idx = 0
    data_list = []
    result_str = ""

    for i in range(rownums):
        data_list.append([])        #[[],[],[],[]]

    for j in range(len(ori_str)):
        print(idx)
        data_list[idx].append(ori_str[j])
        if idx == 0 or idx == rownums - 1: flag = -flag
        idx = idx + flag

    print(data_list)
    for item_list in data_list:
        result_str += "".join(item_list)
    print(result_str)


def shuffle_str_reg(ori_str,restr):
    x = restr[0]
    first_idx_list = []
    for j in range(len(ori_str)):
        if ori_str[j] == x:
            first_idx_list.append(j)

    flag = True

    for t in first_idx_list:
        for i in range(len(restr)):
            if i + t < len(ori_str) and restr[i] == ori_str[i + t]:
                flag = True
                continue
            elif i + t >= len(ori_str) and restr[i] == ori_str[t + i -len(ori_str)]:
                flag = True
                continue
            else:
                flag = False
                break
        if flag == True:
            break
    print(flag)


def list_ranking(ori_list):

    for item_list in ori_list:
        pass

    ori_item_list = []
    pass



if __name__ == "__main__":
    """Z字形变换"""
    # rownums = 3
    # ori_str = 'PAYPALISHIRING'
    # shuffle_Z(rownums,ori_str)


    """字符串轮转"""
    s1 = 'waterbottle'
    s2 = 'erbottlewat'
    # s1 = "aa"
    # s2 = "aba"
    shuffle_str_reg(s1,s2)