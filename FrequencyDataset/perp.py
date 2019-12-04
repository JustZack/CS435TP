import string
from collections import defaultdict
from collections import Counter
import math

class language_model:
    def __init__(self, ngram=1) :
        """
        Initialize a language model

        Parameters:
        ngram specifies the type of model:  
        unigram (ngram = 1), bigram (ngram = 2) etc.
        """
        self.ngram = ngram
        
    def readModel(self, folder) : 
        model = {}
        for i in range(8) :
            text = open(folder + "/part-r-0000" + str(i), 'r').readlines()
            for line in text :
                parts = line.split('\t')
                bg = parts[0]
                fq = parts[1]
                model[bg]= int(fq)
        return model

    def asUnigramFrequency(self, model) :
        unimodel = {}
        for bg in model :
                g = bg.split(' ')
                for n in g :
                    fq = model[bg]
                    if n in unimodel : fq += unimodel[n]
                    unimodel[n] = fq
        return unimodel

    def train(self, folder) :
        """
        train a language model

        Parameters:
        file_name is a file that contains the training set for the model
        """
        #Dont need this
        #self.text = self.getText(file_name)
        if self.ngram > 1 :
            # Change this:
            # Need to read in each part-r-0000x for the bigram frequencys
            self.bigram_data = self.readModel(folder)
            #for i in range(len(self.text) - 1) :
            #    self.bigram_data.append(self.text[i] +' '+ self.text[i+1])
            #self.bigram_data = Counter(self.bigram_data) 

        # Change this:
        # Go through bigram data, and compute unigram (word) frequency
        #
        # word1 word2 freq1
        # word3 word1 freq2
        # 
        # word1: fre1 + fre2
        # ...
        self.frequency_data = self.asUnigramFrequency(self.bigram_data)
        
        self.V = len(self.frequency_data)
        self.total_count = sum(self.frequency_data.values())
        
        pass

    def asBigrams(self, text) :
        # Convert the text to lower case.
        text = text.lower()

        # Convert question marks (?), colons (:) and exclamation marks (!) to periods.
        # Dashes should be converted to spaces.
        text = text.translate({63: 46, 58: 46, 33: 46, 45: 32})

        # Remove all punctuation marks other than the period (commas, semicolons,
        # underscores and quotes).
        trans = str.maketrans('', '', string.punctuation.replace('.', ''))
        text = text.translate(trans)

        # Also replace whitespace with a single space.
        text = ' '.join(text.split())

        # Parse the text into sentences, adding beginning of sentence and end of sentence tokens.
        sentences = text.split('.')
        text = []
        for i in range(len(sentences)) :
            sentences[i] = sentences[i].strip()
            if sentences[i] is not '' :
                text += ['<s>'] + sentences[i].split() + ['</s>']
        
        return text

    def test(self, text) :
        text = self.asBigrams(text)
        
        return self.perplexity(text)
    
    def perplexity(self, text) :
        return math.pow(2, self.entropy(text))
    
    def entropy(self, text) :
        e = 0
        for i in range(self.ngram - 1, len(text)) :
            context = text[i - self.ngram + 1 : i] # This is the previous word/words for the bigram/trigram.
            e += -math.log(self.probability(text[i], context), 2)     
        return e / (len(text) - (self.ngram - 1))
        
    '''Word is a single word. Context is a bi-gram.'''
    def probability(self, word, context) :
        if self.ngram == 1 :
            return (self.count([word]) + 1) / (self.total_count + self.V)
        else :
            return (self.count(context + [word]) + 1) / (self.count(context) + self.V)
    
    def count(self, context) :
        length = len(context)
        context = ' '.join(context)
        if length == 1 :
            if context in self.frequency_data : return self.frequency_data[context]
            else : return 0
            # return self.frequency_data.setdefault(context, 0)
        elif length == 2 :
            if context in self.bigram_data : return self.bigram_data[context]
            else : return 0
            # return self.bigram_data.setdefault(context, 0)
        return 0
    def printPerplexity(self, text) : 
        print(str(self.test(text)) + " : " + text)

gram = 2
model = language_model(2)

print("Training model")
model.train('bigram-frequency')

model.printPerplexity('this is good')
model.printPerplexity('Literally just 19 very large cats')
model.printPerplexity('You wont belive this miracle cream')
model.printPerplexity('this new fad has scientists worried')
model.printPerplexity("14 strangely satisfying videos of melting cheese")
model.printPerplexity( "Is Justin Trudeau the anti-Trump?")
model.printPerplexity("The difference between Donald Trump and Justin Trudeau in two pictures")
model.printPerplexity("15 signs you are, without a doubt, a Target mom")
model.printPerplexity("The rape came to light when the child whispered in the judge's ear about the rape")
