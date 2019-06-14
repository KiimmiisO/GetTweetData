from nltk.cluster.util import cosine_distance
# from pythainlp.corpus import stopwords


def sentence_similarity(sent1, sent2, stopwords=None):
    if stopwords is None:
        stopwords = []

    sent1 = [w.lower() for w in sent1]
    sent2 = [w.lower() for w in sent2]

    all_words = list(set(sent1 + sent2))

    vector1 = [0] * len(all_words)
    vector2 = [0] * len(all_words)

    # build the vector for the first sentence
    for w in sent1:
        if w in stopwords:
            continue
        vector1[all_words.index(w)] += 1

    # build the vector for the second sentence
    for w in sent2:
        if w in stopwords:
            continue
        vector2[all_words.index(w)] += 1

    return 1 - cosine_distance(vector1, vector2)


f_in = open("data/output/word_wrapping.txt", "r", encoding='UTF-8')
count = 0

text_array = [line for line in f_in]

cosine_arrays = []
for text_i in text_array:
    cosine_array = []
    for text_j in text_array:
        # print(text_i)
        # print(text_j)
        cosine_array.append(sentence_similarity(text_i.split(), text_j.split()))
        # print(sentence_similarity(text_i.split(), text_j.split()))
        count += 1
    cosine_arrays.append(cosine_array)
print(count)
print(cosine_arrays)
