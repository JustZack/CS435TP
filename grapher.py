import matplotlib.pyplot as plt
import numpy as np

def get_bins(num_bins, maximum, data) :
    bins = [0]*num_bins
    total = 0
    for p in data :
        if p < maximum :
            bins[int((p / maximum) * num_bins)] += 1
        total += 1

    for i in range(0, len(bins)) :
        bins[i] = (bins[i] / total) * 100
    return bins


non_clickbait_perps = [float(l.split(':')[0].strip()) for l in open('sorted_abcnews_output', 'r').readlines()]
clickbait_perps = [float(l.split(':')[0].strip()) for l in open('sorted_clickbait_output', 'r').readlines()]
# too wide     closer    good     too far   
# 200,000 --> 50,000 --> 12,500 --> 6000 --> 20,000 --> 30,000
maximum = 50000 # Highest actual perplexity was 1137492.336529789, but that squishes the data too much
num_bins = 25
nc_bins = get_bins(num_bins, maximum, non_clickbait_perps)
c_bins = get_bins(num_bins, maximum, clickbait_perps)

ind = np.arange(num_bins)
width = 0.35
plt.bar(ind, nc_bins, width, label='Non-clickbait')
plt.bar(ind+width, c_bins, width, label='Clickbait')

plt.xlabel('Perplexity')
plt.ylabel('% of articles')
plt.title('Perplexity distribution of article headlines')
plt.legend(loc='best')

# You need to change xticks or else they will display 0 to num_bins
plt.xticks(ind + width / 2, (int(((i+1) / num_bins) * maximum) for i in range(num_bins)), rotation='vertical')
plt.subplots_adjust(bottom=0.2)

plt.show()
