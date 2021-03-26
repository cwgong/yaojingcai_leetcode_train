import io
import heapq
import re
import uuid

def stat_duplicate_result(input_file):
    words_dict = {}
    with io.open(input_file,"r",encoding="utf-8") as f1:
        for line in f1:
            if len(line) == 0:continue
            line_list = line.split("\t")
            if len(line_list) != 2:continue
            word,num = line_list

            if word in words_dict:
                words_dict[word] += int(num)
            else:
                words_dict[word] = int(num)

    print(words_dict)
    word_list = [(k, v) for k, v in words_dict.items()]

    word_list_rev = sorted(word_list, key=lambda x: x[1], reverse=True)

    with io.open("./data/seg_result_1.txt", "w", encoding="utf-8") as f2:
        for x in word_list_rev:
            f2.write("%s\t%s\n" % (x[0], x[1]))
        f2.flush()


def extract_regular(ori_title,s_str):
    s_rgx = re.compile(r"(\d+\.?\d*)({0})".format(s_str))
    ori_num_list = s_rgx.findall(ori_title.lower())
    if len(ori_num_list) == 0:
        ori_regular = ''
    else:
        ori_regular = "".join(ori_num_list[-1])

    return ori_regular




def temp():
    nums = [1, 8, 2, 23, 7, -4, 18, 23, 24, 37, 2]
    max_num_index_list = map(nums.index, heapq.nlargest(3, nums))
    print(list(max_num_index_list))



if __name__ == "__main__":
    # input_file = './data/r_stat.txt'
    # stat_duplicate_result(input_file)

    # temp()
    # s_list = ['箱','粒','枚','块','g','kg','袋','片','板','s','ml','l','瓶','条','人份','t','盒','毫升','升','小包','丸','支','个','贴','包']
    # s_str = "|".join(s_list)
    # s = '【非药】(海诺)防水型创可贴 6*20'
    # s2 = '【非药】藏王湿毒膏 15g/支 20支/盒'
    # print(extract_regular(s,s_str))
    # ori_regular = extract_regular(s2,s_str)
    # print(ori_regular)

    print(type(str(uuid.uuid1())))
