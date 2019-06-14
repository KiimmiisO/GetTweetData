import deepcut
from pythainlp import word_tokenize

f_in = open("data/input/tweet_hub.txt", "r", encoding='UTF-8')
f_out = open("data/output/word_wrapping.txt", "w+", encoding='UTF-8')
for text in f_in:
    print(text.replace(' ', ''))
    proc = word_tokenize(text.replace(' ', ''), engine='deepcut')
    for i in proc:
        f_out.write("{} ".format(i))

f_in.close()
f_out.close()


