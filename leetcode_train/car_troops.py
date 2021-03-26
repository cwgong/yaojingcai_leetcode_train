"""
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

************************************************************

102. 二叉树的层序遍历
给你一个二叉树，请你返回其按 层序遍历 得到的节点值。 （即逐层地，从左到右访问所有节点）。



示例：
二叉树：[3,9,20,null,null,15,7],

    3
   / \
  9  20
    /  \
   15   7
返回其层序遍历结果：

[
  [3],
  [9,20],
  [15,7]
]

"""


def cars_troops(target,position,speed):
    time_list = []
    max_time = 0.0
    cars_num = 0
    for i in range(len(position)):
        for j in range(0,len(position) - i - 1):

            if position[j] < position[j + 1]:
                position[j], position[j+1] = position[j+1],position[j]
                speed[j],speed[j + 1] = speed[j+1],speed[j]

    for z in range(len(position)):
        time_list.append((target - position[z])/speed[z])

    for item in time_list:
        if item > max_time:
            max_time = item
            cars_num += 1
        else:
            continue
    print(cars_num)


class Solution(object):
    def levelOrder(self, root):
        # 判断根节点是否为空，是直接返回空列表
        if not root:
            return []
        # 用来记录结果的列表
        res = []

        temp = []
        # 将根节点存放在队列里便于控制循环，
        temp.append(root)

        while len(temp) != 0:
            # 将temp更新成二叉树的每一层节点
            res, temp = self.Travel(temp, res, [])

        return res

    def Travel(self, temp, res, tmp):
        # 存储每一层的值
        temp1 = []
        # 遍历层序节点
        for i in range(len(temp)):

            temp1.append(temp[i].val)

            if temp[i].left != None:
                tmp.append(temp[i].left)
            if temp[i].right != None:
                tmp.append(temp[i].right)
        # 防止本层没有节点的值
        if len(temp1) != 0:
            res.append(temp1)
        return res, tmp



if __name__ == "__main__":
    target = 12
    position = [10, 8, 0, 5, 3]
    speed = [2, 4, 1, 1, 3]
    cars_troops(target,position,speed)