import io

words_dict = {}
with io.open("./redbook_data/3ce_tag.txt","r",encoding="utf-8") as f1:
    for line in f1:
        if len(line) == 0:continue
        line_list = line.strip().split(',')
        for item in line_list:
            if item in words_dict:
                words_dict[item.strip()] += 1
            else:
                words_dict[item.strip()] = 1

print(len(words_dict))

word_list = [(k,v) for k,v in words_dict.items()]
word_list_rev = sorted(word_list,key=lambda x:x[1],reverse=True)
for x in word_list_rev:
    print("%s\t%s"%(x[0],x[1]))