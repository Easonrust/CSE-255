#!/usr/bin/env python
# coding: utf-8

# # Homework 2
# 
# In this homework, we are going to play with Twitter data.
# 
# The data is represented as rows of of [JSON](https://en.wikipedia.org/wiki/JSON#Example) strings.
# It consists of tweets, messages, and a small amount of broken data (cannot be parsed as JSON).
# 
# For this homework, we will only focus on tweets and ignore all other messages.
# 
# 
# ## Tweets
# 
# A tweet consists of many data fields. You can learn all about them in the Twitter API doc. We are going to briefly introduce only the data fields that will be used in this homework.
# 
# * `created_at`: Posted time of this tweet (time zone is included)
# * `id_str`: Tweet ID - we recommend using `id_str` over using `id` as Tweet IDs, becauase `id` is an integer and may bring some overflow problems.
# * `text`: Tweet content
# * `user`: A JSON object for information about the author of the tweet
#     * `id_str`: User ID
#     * `name`: User name (may contain spaces)
#     * `screen_name`: User screen name (no spaces)
# * `retweeted_status`: A JSON object for information about the retweeted tweet (i.e. this tweet is not original but retweeteed some other tweet)
#     * All data fields of a tweet except `retweeted_status`
# * `entities`: A JSON object for all entities in this tweet
#     * `hashtags`: An array for all the hashtags that are mentioned in this tweet
#     * `urls`: An array for all the URLs that are mentioned in this tweet
# 
# 
# ## Data source
# 
# All tweets are collected using the [Twitter Streaming API](https://dev.twitter.com/streaming/overview).
# 
# 
# ## Users partition
# 
# Besides the original tweets, we will provide you with a Pickle file, which contains a partition over 452,743 Twitter users. It contains a Python dictionary `{user_id: partition_id}`. The users are partitioned into 7 groups.

# ## Grading
# 
# We ask you use the `OutputLogger` object `my_output` to store the
# results of your program.
# We have provided function calls to `my_output.append()` method for
# storing the results in all necessary places.
# Please make sure NOT to remove these lines
# 
# In the last cell of this file, we write the content of `my_output`
# to a pickle file which the grader will read in and use for grading.

# In[1]:


import os
import pickle


class OutputLogger:
    def __init__(self):
        self.ans = {}

    def append(self, key, value):
        self.ans[key] = value

    def write_to_disk(self):
        filepath = os.path.expanduser('answer.pickle')
        with open(filepath, 'wb') as f:
            pickle.dump(self.ans, f)


my_output = OutputLogger()


# In[2]:


"""
Variables used in the grading process. DO NOT OVERWRITE THEM.
"""

_is_in_develop = True
_version = '5mb'


# In[3]:


"""
This is a useful cell for debugging.
Use save_time(key) at different parts of your code for checking the amount of time a segment takes.
"""

from time import time
import datetime

timer = {}
def save_time(key):
    '''
    Calling save_time with 'key' the first time will record the current time for 'key'.
    Calling save_time with 'key' the second time will record the time (hours:minutes:seconds) 
    it takes from the first calling to the second calling. All results will be saved
    in timer dict.
    Calling save_time with 'key' the third time will overwrite existing data.
    
    Args:
        key: an identifier for this time.
    '''
    
    if key in timer: 
        if type(timer[key]) == str:
            timer[key] = time()
        else:
            timer[key] = str(datetime.timedelta(seconds=round(time() - timer[key])))
    else:
        timer[key] = time()
                
save_time('total time')


# # Part 0: Load data to a RDD

# In[4]:


from pyspark import SparkContext, SparkConf
from pyspark.rdd import RDD

def set_spark_config(leader_name=None, app_name="cse255 spark"):
    import os
    from pyspark import SparkContext, SparkConf

    def get_local_ip():
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip

    if leader_name is not None:
        # Connect to the treasurer's spark clusters
        os.environ['SPARK_LOCAL_IP'] = "" #driver_host
        driver_host = get_local_ip()

        conf = SparkConf()
        conf.setMaster(f'spark://spark-master.{leader_name}.svc.cluster.local:7077')
        conf.set("spark.blockmanager.port", "50002")
        conf.set("spark.driver.bindAddress", driver_host)
        conf.set("spark.driver.host", driver_host)
        conf.set("spark.driver.port", "50500")
        conf.set('spark.authenticate', False)
    else:
        conf = SparkConf()
        
#     conf.set("spark.cores.max", 4)
    conf.setAppName(app_name)
    sc = SparkContext(conf=conf)

    return sc


# In[5]:


save_time("set up sc")

# Use local clusters (None) while developing in the notebook 
sc = set_spark_config(leader_name=None) 

save_time("set up sc")


# Now let's see how many lines there are in the input file.
# 
# 1. Make RDD from the data in the file given by the file path.
# 2. Mark the RDD to be cached (so in next operation data will be loaded in memory) 
# 3. Count the number of elements in the RDD and store the result in `num_tweets`.
# 
# **Hint:** use [`sc.textFile()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.textFile.html) to read the text file into an RDD. 
# 
# See the visible test below for the expected format and value of `num_tweets`.

# In[6]:


save_time("read data")

file_path = f'file:///datasets/cs255-sp22-a00-public/public/hw2-files-{_version}.txt'

# YOUR CODE HERE
rdd = sc.textFile(file_path).cache()
num_tweets = rdd.count()
# YOUR CODE ENDS

my_output.append("num_tweets", num_tweets)
save_time("read data")


# In[7]:


if _is_in_develop:
    assert num_tweets == 1071, "Your answer is not correct"


# # Part 1: Parse JSON strings to JSON objects

# Python has built-in support for JSON.

# In[8]:


import json

json_example = '''
{
    "id": 1,
    "name": "A green door",
    "price": 12.50,
    "tags": ["home", "green"]
}
'''

json_obj = json.loads(json_example)
json_obj


# ## Broken tweets and irrelevant messages
# 
# The data of this assignment may contain broken tweets (invalid JSON strings). So make sure that your code is robust for such cases.
# 
# You can filter out such broken tweet by checking if:
# * the line is not in json format
# 
# In addition, some lines in the input file might not be tweets, but messages that the Twitter server sent to the developer (such as limit notices). Your program should also ignore these messages.
# 
# These messages would not contain the `created_at` field and can be filtered out accordingly.
# * Check if json object of the broken tweet has a `created_at` field
# 
# **Hint:** [Catch the ValueError](http://stackoverflow.com/questions/11294535/verify-if-a-string-is-json-in-python)
# 
# **********************************************************************************
# 
# **Tasks**
# 
# 1. Parse raw JSON tweets stored in the RDD you created above to obtain **valid** JSON objects. 
# 1. From all valid tweets, construct a pair RDD of `(user_id, text)`, where `user_id` is the `id_str` data field of the `user` dictionary (read [Tweets](#Tweets) section above), `text` is the `text` data field.

# In[9]:


import json

def safe_parse(raw_json):
    """
    Input is a String
    Output is a JSON object if the tweet is valid and None if not valid
    """

    # YOUR CODE HERE
    try:
        json_object = json.loads(raw_json)
        json_object["created_at"]
        return json_object
    except:
        return None
    # YOUR CODE ENDS


# In[10]:


# Remember to construct an RDD of (user_id, text) here.

# YOUR CODE HERE
pairs_rdd = rdd.map(safe_parse).filter(lambda x: x is not None).map(lambda x:(x["user"]["id_str"], x["text"])).cache()
# YOUR CODE ENDS


# ## Number of unique users
# 
# Count the number of different users in all valid tweets and store the result in `num_unique_users`.
# 
# **Hint:** use the [`distinct()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.distinct.html) method.
# 
# See the visible test below for the expected format and value of `num_unique_users`.

# In[11]:


save_time("count unique users")

# YOUR CODE HERE
num_unique_users = pairs_rdd.map(lambda x: x[0]).distinct().count()
# YOUR CODE ENDS

my_output.append("num_unique_users", num_unique_users)
save_time("count unique users")


# In[12]:


if _is_in_develop:
    assert num_unique_users == 910, "Your answer is not correct"


# # Part 2: Number of posts from each user partition

# Load the Pickle file `users-partition.pickle`, you will get a dictionary which represents a partition over 452,743 Twitter users, `{user_id: partition_id}`. The users are partitioned into 7 groups. For example, if the dictionary is loaded into a variable named `partition`, the partition ID of the user `59458445` is `partition["59458445"]`. These users are partitioned into 7 groups. The partition ID is an integer between 0-6.
# 
# Note that the user partition we provide doesn't cover all users appear in the input data.

# In[13]:


import subprocess
import pickle

proc = subprocess.Popen(["cat", "./users-partition.pickle"], stdout=subprocess.PIPE)
pickle_content = proc.communicate()[0]

partition = pickle.loads(pickle_content)
len(partition)


# ## Tweets per user partition
# 
# 1. Count the number of posts from group 0, 1, ..., 6, plus the number of posts from users who are not in any partition. Assign users who are not in any partition to the group 7.
# 1. Put the results of this step into a pair RDD `(group_id, count)` that is sorted by key.
# 1. Collect the RDD to get `counts_per_part` list.
# 
# See the visible test below for the expected format and value of `counts_per_part`.

# In[14]:


save_time("count tweets per user partition")

# YOUR CODE HERE
partition_rdd = pairs_rdd.map(lambda x: (partition.get(x[0], 7), 1)).reduceByKey(lambda x,y: x+y).sortByKey()
counts_per_part = partition_rdd.collect()
# YOUR CODE ENDS

my_output.append("counts_per_part", counts_per_part)
save_time("count tweets per user partition")


# In[15]:


if _is_in_develop:
    assert counts_per_part == [
        (0, 40),
        (1, 116),
        (2, 16),
        (3, 199),
        (4, 56),
        (5, 176),
        (6, 217),
        (7, 245)
    ], "Your answer is not correct"


# # Part 3:  Tokens that are relatively popular in each user partition

# In this step, we are going to find tokens that are relatively popular in each user partition.
# 
# We define the number of mentions of a token $t$ in a specific user partition $k$ as the number of users from the user partition $k$ that ever mentioned the token $t$ in their tweets. Note that even if some users might mention a token $t$ multiple times or in multiple tweets, a user will contribute at most 1 to the counter of the token $t$.
# 
# Please make sure that the number of mentions of a token is equal to the number of users who mentioned this token but NOT the number of tweets that mentioned this token.
# 
# Let $N_t^k$ be the number of mentions of the token $t$ in the user partition $k$. Let $N_t^{all} = \sum_{i=0}^7 N_t^{i}$ be the number of total mentions of the token $t$.
# 
# We define the relative popularity of a token $t$ in a user partition $k$ as the log ratio between $N_t^k$ and $N_t^{all}$, i.e. 
# 
# \begin{equation}
# p_t^k = \log \frac{N_t^k}{N_t^{all}}.
# \end{equation}
# 
# 
# You can compute the relative popularity by calling the function `get_rel_popularity`.

# We load a tweet tokenizer for you in the following cells. This Tokenizer object is called `tok`. Don't forget to execute the two cells below.

# In[16]:


#!/usr/bin/env python

"""
This code implements a basic, Twitter-aware tokenizer.

A tokenizer is a function that splits a string of text into words. In
Python terms, we map string and unicode objects into lists of unicode
objects.

There is not a single right way to do tokenizing. The best method
depends on the application.  This tokenizer is designed to be flexible
and this easy to adapt to new domains and tasks.  The basic logic is
this:

1. The tuple regex_strings defines a list of regular expression
   strings.

2. The regex_strings strings are put, in order, into a compiled
   regular expression object called word_re.

3. The tokenization is done by word_re.findall(s), where s is the
   user-supplied string, inside the tokenize() method of the class
   Tokenizer.

4. When instantiating Tokenizer objects, there is a single option:
   preserve_case.  By default, it is set to True. If it is set to
   False, then the tokenizer will downcase everything except for
   emoticons.

The __main__ method illustrates by tokenizing a few examples.

I've also included a Tokenizer method tokenize_random_tweet(). If the
twitter library is installed (http://code.google.com/p/python-twitter/)
and Twitter is cooperating, then it should tokenize a random
English-language tweet.


Julaiti Alafate:
  I modified the regex strings to extract URLs in tweets.
"""

__author__ = "Christopher Potts"
__copyright__ = "Copyright 2011, Christopher Potts"
__credits__ = []
__license__ = "Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Unported License: http://creativecommons.org/licenses/by-nc-sa/3.0/"
__version__ = "1.0"
__maintainer__ = "Christopher Potts"
__email__ = "See the author's website"

######################################################################

import re
from html import entities 

######################################################################
# The following strings are components in the regular expression
# that is used for tokenizing. It's important that phone_number
# appears first in the final regex (since it can contain whitespace).
# It also could matter that tags comes after emoticons, due to the
# possibility of having text like
#
#     <:| and some text >:)
#
# Most imporatantly, the final element should always be last, since it
# does a last ditch whitespace-based tokenization of whatever is left.

# This particular element is used in a couple ways, so we define it
# with a name:
emoticon_string = r"""
    (?:
      [<>]?
      [:;=8]                     # eyes
      [\-o\*\']?                 # optional nose
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth      
      |
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
      [\-o\*\']?                 # optional nose
      [:;=8]                     # eyes
      [<>]?
    )"""

# The components of the tokenizer:
regex_strings = (
    # Phone numbers:
    r"""
    (?:
      (?:            # (international)
        \+?[01]
        [\-\s.]*
      )?            
      (?:            # (area code)
        [\(]?
        \d{3}
        [\-\s.\)]*
      )?    
      \d{3}          # exchange
      [\-\s.]*   
      \d{4}          # base
    )"""
    ,
    # URLs:
    r"""http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"""
    ,
    # Emoticons:
    emoticon_string
    ,    
    # HTML tags:
     r"""<[^>]+>"""
    ,
    # Twitter username:
    r"""(?:@[\w_]+)"""
    ,
    # Twitter hashtags:
    r"""(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"""
    ,
    # Remaining word types:
    r"""
    (?:[a-z][a-z'\-_]+[a-z])       # Words with apostrophes or dashes.
    |
    (?:[+\-]?\d+[,/.:-]\d+[+\-]?)  # Numbers, including fractions, decimals.
    |
    (?:[\w_]+)                     # Words without apostrophes or dashes.
    |
    (?:\.(?:\s*\.){1,})            # Ellipsis dots. 
    |
    (?:\S)                         # Everything else that isn't whitespace.
    """
    )

######################################################################
# This is the core tokenizing regex:
    
word_re = re.compile(r"""(%s)""" % "|".join(regex_strings), re.VERBOSE | re.I | re.UNICODE)

# The emoticon string gets its own regex so that we can preserve case for them as needed:
emoticon_re = re.compile(regex_strings[1], re.VERBOSE | re.I | re.UNICODE)

# These are for regularizing HTML entities to Unicode:
html_entity_digit_re = re.compile(r"&#\d+;")
html_entity_alpha_re = re.compile(r"&\w+;")
amp = "&amp;"

######################################################################

class Tokenizer:
    def __init__(self, preserve_case=False):
        self.preserve_case = preserve_case

    def tokenize(self, s):
        """
        Argument: s -- any string or unicode object
        Value: a tokenize list of strings; conatenating this list returns the original string if preserve_case=False
        """        
        # Try to ensure unicode:
        try:
            s = str(s)
        except UnicodeDecodeError:
            s = s.encode('string_escape')
            s = str(s)
        # Fix HTML character entitites:
        s = self.__html2unicode(s)
        # Tokenize:
        words = word_re.findall(s)
        # Possible alter the case, but avoid changing emoticons like :D into :d:
        if not self.preserve_case:            
            words = map((lambda x : x if emoticon_re.search(x) else x.lower()), words)
        return words

    def tokenize_random_tweet(self):
        """
        If the twitter library is installed and a twitter connection
        can be established, then tokenize a random tweet.
        """
        try:
            import twitter
        except ImportError:
            print("Apologies. The random tweet functionality requires the Python twitter library: http://code.google.com/p/python-twitter/")
        from random import shuffle
        api = twitter.Api()
        tweets = api.GetPublicTimeline()
        if tweets:
            for tweet in tweets:
                if tweet.user.lang == 'en':            
                    return self.tokenize(tweet.text)
        else:
            raise Exception("Apologies. I couldn't get Twitter to give me a public English-language tweet. Perhaps try again")

    def __html2unicode(self, s):
        """
        Internal metod that seeks to replace all the HTML entities in
        s with their corresponding unicode characters.
        """
        # First the digits:
        ents = set(html_entity_digit_re.findall(s))
        if len(ents) > 0:
            for ent in ents:
                entnum = ent[2:-1]
                try:
                    entnum = int(entnum)
                    s = s.replace(ent, unichr(entnum))	
                except:
                    pass
        # Now the alpha versions:
        ents = set(html_entity_alpha_re.findall(s))
        ents = filter((lambda x : x != amp), ents)
        for ent in ents:
            entname = ent[1:-1]
            try:            
                s = s.replace(ent, unichr(entities.name2codepoint[entname]))
            except:
                pass                    
            s = s.replace(amp, " and ")
        return s


# In[17]:


from math import log

tok = Tokenizer(preserve_case=False)

def get_rel_popularity(c_k, c_all):
    '''
    Compute the relative popularity of a token.
    
    Args:
        c_k: the number of mentions in the user partition k.
        c_all: the number of all mentions.
        
    Return:
        The relative popularity of the token. It should be a negative number due to the log function. 
    '''
    
    return log(1.0 * c_k / c_all) / log(2)


# ## Tokenize tweets
# 
# 1. Tokenize the tweets using the `tokenize` function that is a method of the `Tokenizer` class that we have instantiated as `tok`. 
# 1. Count the number of mentions for each tokens regardless of specific user group and store them in a RDD, which will be used later.
# 1. Get `num_of_tokens`, which is how many different tokens we have.
# 
# See the visible test below for the expected format and value of `num_of_tokens`.

# In[18]:


save_time("count all unique tokens")

# YOUR CODE HERE
tokens_rdd = pairs_rdd.flatMap(lambda x: [(x[0],token) for token in tok.tokenize(x[1])]).distinct().cache()
freq_tokens_rdd = tokens_rdd.map(lambda x:(x[1],1)).reduceByKey(lambda x, y: x+y).cache()
num_of_tokens = freq_tokens_rdd.map(lambda x:x[0]).distinct().count()
# YOUR CODE ENDS

my_output.append("num_of_tokens", num_of_tokens)
save_time("count all unique tokens")


# In[19]:


if _is_in_develop:
    assert num_of_tokens == 4724, "Your answer is not correct"


# ## Token popularity
# 
# Tokens that are mentioned by too few users are usually not very interesting, so we want to only keep tokens that are mentioned by at least 100 users. Filter out tokens that don't meet this requirement. Compute the two varaibles below.
# 
# `num_freq_tokens`: how many different tokens we have after the filtering. 
# 
# `top20`: the top 20 most frequent tokens after the filtering.
# 
# See the visible tests below for the expected format and value of `num_freq_tokens` and `top20`.

# In[20]:


save_time("count overall most popular tokens")

# YOUR CODE HERE
freq_tokens_rdd = freq_tokens_rdd.filter(lambda x:x[1]>=100).cache()
num_freq_tokens = freq_tokens_rdd.count()
top20 = freq_tokens_rdd.sortBy(lambda x: -x[1]).take(20)
# YOUR CODE ENDS

my_output.append("num_freq_tokens", num_freq_tokens)
my_output.append("top20", top20)
save_time("count overall most popular tokens")


# In[21]:


if _is_in_develop:
    assert num_freq_tokens == 18, "Your answer is not correct"


# In[22]:


if _is_in_develop:
    assert top20 == [
        (':', 558),
        ('rt', 499),
        ('.', 397),
        ('the', 305),
        ('trump', 284),
        ('to', 271),
        ('…', 267),
        (',', 245),
        ('is', 192),
        ('in', 189),
        ('a', 179),
        ('of', 149),
        ('for', 149),
        ('!', 145),
        ('and', 132),
        ('i', 118),
        ('on', 118),
        ('he', 100)
    ], "Your answer is not correct"


# ## Relative Popularity
# 
# For all tokens that are mentioned by at least 100 users, compute their relative popularity in each user group. Then get the top 10 tokens with highest relative popularity in each user group and store the results in the list `popular_10_in_each_group`. In case two tokens have same relative popularity, break the tie by printing the alphabetically smaller one.
# 
# **Hint:** Let the relative popularity of a token $t$ be $p$. The order of the items will be satisfied by sorting them using (-p, t) as the key.
# 
# See the visible test below for the expected format and value of `popular_10_in_each_group`.

# In[24]:


save_time("print popular tokens in each group")

# YOUR CODE HERE
freq_tokens_map = freq_tokens_rdd.collectAsMap()
popular_10_in_each_group_rdd = tokens_rdd.filter(lambda x:x[1] in freq_tokens_map)                    .map(lambda x:((partition.get(x[0],7), x[1]), 1))                    .reduceByKey(lambda x,y :x+y)                    .map(lambda x: (x[0][0], (x[0][1], get_rel_popularity(x[1],freq_tokens_map[x[0][1]]))))                    .groupByKey(8).mapValues(list).map(lambda x:x[1])                    .map(lambda x: sorted(x,key=lambda a: (-a[1],a[0])))                    .map(lambda x: x[:10])
popular_10_in_each_group = popular_10_in_each_group_rdd.collect()
# YOUR CODE ENDS

my_output.append("popular_10_in_each_group", popular_10_in_each_group)
save_time("print popular tokens in each group")


# In[25]:


if _is_in_develop:
    assert popular_10_in_each_group == [
        [('and', -4.23703919730085),
         (',', -4.351675438281414),
         ('to', -4.38170932321278),
         ('for', -4.4118135984045574),
         ('i', -4.560714954474479),
         ('.', -4.632995197142958),
         ('of', -4.634206019741006),
         ('the', -4.667702931729092),
         (':', -4.876193798385603),
         ('a', -4.8988532765431)],
        [('for', -2.6342060197410055),
         ('a', -2.783376059123164),
         ('rt', -2.792971003894948),
         ('.', -2.851635483618298),
         (':', -2.876193798385602),
         ('is', -2.8845227825800643),
         ('to', -2.912224039911559),
         ('…', -2.931412914742588),
         ('!', -3.0099840885726223),
         ('and', -3.0443941193584534)],
        [('in', -4.754887502163469),
         ('the', -5.252665432450248),
         ('to', -5.497186540632716),
         ('is', -5.584962500721157),
         (',', -5.614709844115209),
         ('of', -5.634206019741006),
         ('rt', -5.640967910449898),
         ('he', -5.643856189774724),
         ('…', -5.738767836800192),
         (':', -5.802193216941825)],
        [('and', -2.584962500721156),
         ('rt', -2.659115257160158),
         ('of', -2.695606564405149),
         (':', -2.6978565571270896),
         ('…', -2.7387678368001915),
         ('to', -2.7967468224916234),
         ('.', -2.8256402750853535),
         ('is', -2.9411063109464317),
         ('a', -2.960253821207244),
         ('the', -2.967263213588)],
        [('for', -3.6342060197410055),
         (',', -3.6887104255589853),
         ('in', -3.7548875021634687),
         ('of', -3.7597369018248643),
         ('he', -3.8365012677171206),
         ('i', -3.8826430493618416),
         ('on', -3.8826430493618416),
         ('to', -3.912224039911559),
         ('the', -3.9307373375628862),
         ('.', -3.932555479001866)],
        [('on', -1.838248930003388),
         ('he', -2.1844245711374275),
         ('i', -2.2387868595871168),
         ('is', -2.299560281858908),
         ('.', -2.311067102255595),
         ('…', -2.332775477124355),
         ('a', -2.35453276031929),
         ('!', -2.3725541679573303),
         ('the', -2.3946844373226766),
         ('rt', -2.5705785825585004)],
        [('!', -1.398549376490275),
         ('trump', -1.5350372753894739),
         ('…', -1.6683785089087937),
         ('in', -1.7293524100563311),
         ('he', -1.8365012677171204),
         ('is', -1.9411063109464313),
         (',', -2.0297473433940523),
         ('rt', -2.0680782420293173),
         (':', -2.1354366250570216),
         ('and', -2.137503523749935)],
        [('trump', -1.723482364802584),
         ('of', -1.8268510976834016),
         ('for', -2.009715154833212),
         ('i', -2.0246620542342693),
         ('the', -2.1029183129455666),
         ('a', -2.1262637726461726),
         ('in', -2.1699250014423126),
         ('to', -2.175258445745353),
         ('he', -2.1844245711374275),
         ('and', -2.1864131242308815)]
    ], "Your answer is not correct"


# ## Important: Write your solutions to disk
# 
# Following cell write your solutions to disk which would be read in by the grader for grading.

# In[26]:


my_output.write_to_disk()


# ## Runtime statistics of your implementation

# In[27]:


save_time('total time')

for key in timer:
    print(key, timer[key])


# ## Don't forget to convert your notebook to a python script

# In[ ]:


# Convert your HW2.ipynb to HW2.py
if _is_in_develop:
    get_ipython().system('jupyter nbconvert --to script HW2.ipynb')

