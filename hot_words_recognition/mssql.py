import io

# f2 = io.open("./data/jiajuriyong.txt","w",encoding="utf-8")
# with io.open("./data/jiajuriyong_simple.txt","r",encoding="utf-8") as f1:
#     for line in f1:
#         line_ = line.strip()
#         line_list = line_.split("\t")
#         f2.write(line_list[0] + "\n")
# f2.close()

def generate_dict(input_file):
    variety_list = []
    with io.open(input_file,"r",encoding="utf-8") as f1:
        for line in f1:
            tmp_dict = {}
            if line == "":continue
            line_list = line.strip().split("\001")
            if len(line_list) != 7:continue
            tmp_dict["title"] = line_list[4]
            tmp_dict["brand_name"] = line_list[6]
            tmp_dict['gmv'] = float(line_list[5])
            variety_list.append(tmp_dict)
    return variety_list


def match_keywords(input_file,variety_dict,output_file):
    key_words = []
    word_dict = {}
    word_list = []
    with io.open(input_file,"r",encoding="utf-8") as f1:
        for line in f1:
            if line == "":continue
            key_words.append(line.strip())
    # for word in key_words:
    #     gmv = 0.0
    #     for sen_dict in variety_dict:
    #         if len(sen_dict['title'].split(word)) > 1:
    #             gmv = gmv + sen_dict['gmv']
    #     word_dict[word] = gmv

    hot_words_dict = {}
    hotwords_list = {}
    for sen_dict in variety_dict:
        for word in key_words:
            if len(sen_dict['title'].split(word)) > 1:
                if sen_dict['brand_name'] in hot_words_dict:
                    if word in hot_words_dict[sen_dict['brand_name']]:
                        hot_words_dict[sen_dict['brand_name']][word] = hot_words_dict[sen_dict['brand_name']][word] + sen_dict['gmv']
                    else:
                        hot_words_dict[sen_dict['brand_name']][word] = sen_dict['gmv']
                else:
                    hot_words_dict[sen_dict['brand_name']]  = {word:sen_dict['gmv']}

    if hot_words_dict != {}:
        for k,v in hot_words_dict.items():
            tmp_list = [(k_,v_) for k_,v_ in v.items()]
            sorted_tmp_list = sorted(tmp_list,key = lambda x:x[1],reverse=True)
            hotwords_list[k] = sorted_tmp_list

    with io.open(output_file, "w", encoding="utf-8") as f2:
        for k,v in hotwords_list.items():
            for item in v:
                f2.write("%s\t%s\t%s\n"%(k,item[0],item[1]))

    # if word_dict != {}:
    #     word_list = [(k,v) for k,v in word_dict.items()]
    # word_list = sorted(word_list, key=lambda x: x[1], reverse=True)
    # with io.open(output_file,"w",encoding="utf-8") as f2:
    #     for x in word_list:
    #         f2.write(x[0] + "\t" + str(x[1]) + "\n")

def hot_word():
    res_list = []

    label = "其他"
    pos_data_str = "点赞，仔细，服务到位，超赞，良心，棒棒哒，很赞，种草，推荐，回购"
    pos_data_list = pos_data_str.strip().split("，")
    neg_data_str = "很久，好久，难受，无语，遗憾，瑕疵，差评"
    neg_data_list = neg_data_str.strip().split("，")
    for item in pos_data_list:
        res_list.append("%s\t%s|%s"%(item,label,"1"))
    if len(neg_data_list) != 0:
        for item in neg_data_list:
            res_list.append("%s\t%s|%s"%(item,label,"0"))

    with io.open("./data/label_result.txt","a",encoding="utf-8") as f1:
        f1.write("\n".join(res_list))

def words_check():
    data_dict = {}
    with io.open("./data/label_result.txt","r",encoding="utf-8") as f1:
        for line in f1:
            if len(line) == 0:continue
            data_dict[line.split("\t")[0]] = ''
    print(len(data_dict))
    print(data_dict)


def split_word(input_file):

    short_list = []
    long_list = []
    with io.open(input_file,"r",encoding="utf-8") as f1:
        for line in f1:
            flag = True
            if len(line) == 0:
                continue
            line_list = line.strip().split("\t")
            if len(line_list) != 3:continue
            if "/" in line_list[1]:
                for item in line_list[1].strip().split("/"):
                    if len(item) <= 3:
                        flag = False
            else:
                if len(line_list[1]) <= 3:
                    flag = False
            if flag == False:
                short_list.append(line)
            else:
                long_list.append(line)

    with io.open("./redbook_data/jiajvriyong_short_res.txt","w",encoding="utf-8") as f2:
        f2.write("".join(short_list))
    with io.open("./redbook_data/jiajvriyong_long_res.txt","w",encoding="utf-8") as f3:
        f3.write("".join(long_list))


if __name__ == "__main__":
    # input_file_ori = "./data_demo_ik/ori_yiyaobaojian.txt"
    # input_file_words = "./热词/yiyaobaojian.txt"
    # output_file = "./result_data_update/yiyaobaojian.txt"
    # variety_list = generate_dict(input_file_ori)
    # match_keywords(input_file_words,variety_list,output_file)

    # hot_word()
    # words_check()
    split_word("./redbook_data/qicheyongpin_redbook_brand.txt")