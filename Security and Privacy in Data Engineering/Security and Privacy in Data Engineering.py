# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Security & Privacy in Workflows
# MAGIC
# MAGIC In this notebook, we'll explore ways to incorporate data security and privacy into large-scale data workflows. First, you might be wondering why we need to worry about these topics. Shouldn't this be a problem solved by the privacy department, infosec or via product owners? 
# MAGIC
# MAGIC You can think of large scale data workflows like folks who manage the internet. We don't often see their work, but we know when it's broken! They probably deserve a lot more credit and attention for it, but we somehow just expect it "to work ubiquitously." And we certainly expect the data we send around the internet to be kept private and secure (although in some geographies it is less likely...). If it wasn't for the work on those large-scale packet pipelines, then we couldn't trust technologies like SSL, TLS or our web applications or mobile applications. Those are enabled, propogated and enforced by all the intermediary hops, meaning the packet and data are handled with the same promises as they arrived. Hopefully you are getting the picture here -- security and privacy have to be baked into the architecture and data flow from the start, and cannot be simply "tacked on" at a given endpoint.
# MAGIC
# MAGIC So now we understand our responsibilities as the folks building the data backbones. What privacy and security concerns do we actually have? We'll walk through a concrete example to have a look!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example Data Product: Ingest Air Quality Sensors while Protecting User Privacy
# MAGIC
# MAGIC - We want to ingest air quality sensor data from users, buildings and institutions who are willing to send us data to build an air quality map (similar to the [IQAir map](https://www.iqair.com/air-quality-map).
# MAGIC - Users only want to share the data if they can remain anonymous and their location is fuzzy, so that they are protected against stalkers, prying eyes and state surveillance.
# MAGIC - Since the data is sensitive (from people and their homes!), we want to sure that it is secured either at collection, as well as at any intermediary hops.
# MAGIC
# MAGIC Let's first take a look at our data and determine what can and should be done...

# COMMAND ----------

# MAGIC %run ./init

# COMMAND ----------

import pandas as pd
from IPython.display import Image
import os

cwd = os.getcwd()
root_dir = ""

if (cwd == "/databricks/driver"):
    root_dir = f"/dbfs{working_directory}/"
else:
    root_dir = ""

df = pd.read_csv(f'{root_dir}/data/air_quality.csv')

# COMMAND ----------

df.head()

# COMMAND ----------

df.location[1]

# COMMAND ----------

def parse_location(location_string):
    location_keys = ['lat', 'long', 'city', 'country', 'timezone']
    location_vals = [substring for substring in location_string.split("'") 
                     if len(substring.replace(' ','').replace(',', '')) > 1]
    zipped = zip(location_keys, location_vals)
    return dict(zipped)

# COMMAND ----------

parse_location(df.location[1])

# COMMAND ----------

df['location'] = df.location.map(parse_location)

# COMMAND ----------

pd.json_normalize(df.location)

# COMMAND ----------

loc_df = pd.json_normalize(df.location)

# COMMAND ----------

cleaned_df = pd.concat([df, loc_df], axis=1)

# COMMAND ----------

cleaned_df = cleaned_df.drop(['location'], axis=1)

# COMMAND ----------

cleaned_df.head()

# COMMAND ----------

def score_category(aqi):
    if aqi <= 50:
        return 1
    elif aqi <= 100:
        return 2
    elif aqi <= 150:
        return 3
    elif aqi <= 200:
        return 4
    elif aqi <= 300:
        return 5
    return 6

# COMMAND ----------

cleaned_df['air_quality_category'] = cleaned_df.air_quality_index.map(score_category)

# COMMAND ----------

cleaned_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### So what even is sensitive information?
# MAGIC
# MAGIC Categories of sensitive information:
# MAGIC
# MAGIC - **Personally Identifiable Information (PII)**: This is information that we can directly link to a person without much effort. This includes information like email address, IP address, legal name, address, birth date, gender and so forth. Even just one of these pieces of information can be enough to directly identify someone in a dataset.
# MAGIC - **Person-Related Information**: This is data that is created by a person and that likely has some personal artifacts. For example, [web browsing histories are fairly unique](https://blog.lukaszolejnik.com/web-browsing-histories-are-private-personal-data-now-what/), so is location data (i.e. Where do you sleep at night? Where do you work?) and even your likes on social media can statistically reveal sensitive attributes, such as your gender, ethnicity and your political preferences.
# MAGIC - **Confidential Information**: This is sensitive information for companies, that should remain protected via similar methods as personal data. This data could reveal details about the core business model, proprietary practices, customer details (which can also contain personal information!) and internal business processes.
# MAGIC
# MAGIC When we define sensitive information as only PII, we tend to ignore other potential targets of sensitive data, that might be just as, if not more valuable!

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is sensitive here?

# COMMAND ----------

cleaned_df.head()

# COMMAND ----------

cleaned_df.info()

# COMMAND ----------

# MAGIC %md
# MAGIC #### How might we...?
# MAGIC
# MAGIC - Protect user_id while still allowing it to be linkable?
# MAGIC - Remove potentially identifying precision in location?
# MAGIC - Remove potentially identifying information in the timestamp?
# MAGIC - Make these into scalable and repeatable actions for our workflow?
# MAGIC
# MAGIC Let's work on these step by step!

# COMMAND ----------

from ff3 import FF3Cipher
key = "2DE79D232DF5585D68CE47882AE256D6"
tweak = "CBD09280979564"

c6 = FF3Cipher.withCustomAlphabet(key, tweak, 
                                  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_")

plaintext = "michael______"
ciphertext = c6.encrypt(plaintext)

ciphertext

# COMMAND ----------

decrypted = c6.decrypt(ciphertext)
decrypted

# COMMAND ----------

def encrypt_username(username):
    return c6.encrypt(username)

# COMMAND ----------

cleaned_df['user_id'] = cleaned_df.user_id.map(encrypt_username)

# COMMAND ----------

# MAGIC %md
# MAGIC **Oh no! What happened here???**

# COMMAND ----------

def add_padding_and_encrypt(username):
    while len(username) <= 3:
        username = username + " "

    return username
 

# COMMAND ----------

def add_padding_and_encrypt(username):
    if len(username) < 4:
        username += "X" * (4-len(username))
    return c6.encrypt(username)

# COMMAND ----------

cleaned_df['user_id'] = cleaned_df.user_id.map(add_padding_and_encrypt)

# COMMAND ----------

cleaned_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC We are now technically leaking length information... which we could determine is okay, so long as access to this data and the real data is fairly controlled. We could also say that we want to by default add padding to every username to make them consistent. This would be a good homework exercise (and also to write a function to decrypt and remove padding!!). One challenge, what happens if my username ends in X??? :) 
# MAGIC
# MAGIC
# MAGIC Now we can move onto our GPS data!
# MAGIC
# MAGIC How precise is GPS data anyways? 🤔 (from [wikipedia](https://en.wikipedia.org/wiki/Decimal_degrees))
# MAGIC
# MAGIC
# MAGIC decimal places  | degrees  |distance
# MAGIC ------- | -------          |--------
# MAGIC 0        |1                |111  km
# MAGIC 1        |0.1              |11.1 km
# MAGIC 2        |0.01             |1.11 km
# MAGIC 3        |0.001            |111  m
# MAGIC 4        |0.0001           |11.1 m
# MAGIC 5        |0.00001          |1.11 m
# MAGIC 6        |0.000001         |11.1 cm
# MAGIC 7        |0.0000001        |1.11 cm
# MAGIC 8        |0.00000001       |1.11 mm

# COMMAND ----------

cleaned_df.lat

# COMMAND ----------

def reduce_precision(val, degrees=3):
    return round(val, degrees)

# COMMAND ----------

cleaned_df['lat'] = cleaned_df.lat.map(reduce_precision)

# COMMAND ----------

# MAGIC %md
# MAGIC **Oh no! What happened here???**

# COMMAND ----------

cleaned_df['lat'] = cleaned_df['lat'].astype('float').map(reduce_precision)
cleaned_df['long'] = cleaned_df['long'].astype('float').map(reduce_precision)

# COMMAND ----------

cleaned_df['lat'] = cleaned_df['lat'].astype('float').map(reduce_precision)
cleaned_df['long'] = cleaned_df['long'].astype('float').map(reduce_precision)


# COMMAND ----------

cleaned_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC What type of risk should we be aware of with regard to timestamp precision? When and how do we need to de-risk this  type of information?

# COMMAND ----------

def add_small_time_noise(timestamp):
    #TODO
    pass

# COMMAND ----------

from random import uniform
from datetime import timedelta

def add_small_time_noise(timestamp):
    return timestamp + timedelta(seconds=uniform(1,60), minutes=uniform(1,20))

# COMMAND ----------

cleaned_df['timestamp'] = pd.to_datetime(cleaned_df['timestamp']).map(add_small_time_noise)


# COMMAND ----------

cleaned_df.head()

# COMMAND ----------

cleaned_df.to_csv(f'{root_dir}/data/data_for_marketing.csv')

# COMMAND ----------

Image(url="https://github.com/data-derp/exercise-data-security/blob/master/images/cia_triad.png?raw=true", width=400, height=400)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This is a graphic from Wikipedia showing the so-called "CIA" triad, showing some of the core concepts we want to ensure to guarantee data security. Let's review them together:
# MAGIC
# MAGIC - **Confidentiality:** Data is kept confidential, meaning only those who should be able to access it can do so, and fine-tuned access is available and enforced.
# MAGIC - **Integrity:** Data is accurate and cannot easily be changed or tampered with by internal or external actors in a malicious way.
# MAGIC - **Availability:** Data fulfills any service-level objectives (SLOs) or service-level agreements (SLAs) and is made available in a secure and user-friendly manner. 
# MAGIC
# MAGIC So translated into data engineering context, this means that:
# MAGIC
# MAGIC - Our data workflows enforce access-control restrictions, data protections or minimizations related to confidentiality and ensure sinks and sources match the encryption requirements we expect for the data sensitivity.
# MAGIC - Our data workflows do not mangle data, maintain data quality principles outlined by governance processes and alert should malicious activity be detected.
# MAGIC - Our data wofkflows meet SLOs/SLAs outlined by the data consumers and dependant data products.

# COMMAND ----------

# MAGIC %md
# MAGIC ### What about Privacy? 🦹🏻
# MAGIC
# MAGIC A foundational concept when it comes to designing privacy-respecting systems is the Privacy by Design principles outlined by [Anne Cavoukian in 2006](https://iapp.org/media/pdf/resource_center/pbd_implement_7found_principles.pdf).
# MAGIC
# MAGIC Let's pull out a few of the principles that relate to our work as data engineers...
# MAGIC
# MAGIC - **Proactive not Reactive; Preventative not Remedial:** Privacy is built into our architecture and data flows as we start building them. Think of this as the privacy version of TDD -- we write the privacy requirements first and design and build systems to fit them, not the other way around!
# MAGIC - **Privacy as the Default Setting:** We optimize systems so that privacy is on by default, and changes to that are user-driven! This means tracking things like consent, implementing processes for data minimization and ensuring lineage and governance data is available to data consumers or dependant data products.
# MAGIC - **Full Functionality – Positive-Sum, not Zero-Sum:** Data privacy is a benefit for the business, technologists and users, meaning we ensure that it is not a tradeoff in our product design. Users who choose privacy protections (or users who have them on automatically, by default, right?) receive full functionality.
# MAGIC - **End-to-End Security – Full Lifecycle Protection:** Data is secured properly and for it's entire lifecycle (from collection endpoint to deletion!). Here is our big intersection with the security requirements.
# MAGIC
# MAGIC
# MAGIC What does this mean for our data engineering work?
# MAGIC
# MAGIC - Our data workflows have privacy protections outlined and architected in before any code is written. We test for these and ensure they are working properly, should anything change.
# MAGIC - Privacy is turned on by default, and any "unknown" data flows have privacy added to them when they enter into our systems or are discovered (e.g. in cases of unknown data storages or data from third parties).
# MAGIC - We work directly with data producers and consumers (and other stakeholders, such as legal or privacy professionals) to find sweet spots that offer the appropriate protection for users and utility for business needs. Approach this as a postive-sum game and remember that user-centric choices are always a good business investment.
# MAGIC - We design secure workflows that ensure that all user-related or person-related data is properly secured using standards from data security best practices (like our CIA triad!)
# MAGIC
# MAGIC
# MAGIC #### Privacy and Information Continuum
# MAGIC
# MAGIC One useful way to begin shifting your understanding of privacy is to start thinking about it as a point on a spectrum instead of something that is "on" or "off". Here we can see that we can have a full range of points on a continuum, where privacy and information are somewhat at odds with one another. When we have full information, we have no privacy guarantees. When we have complete privacy, we cannot do our job as data people! Finding the right balance is the difficult and fun part of privacy in data science!

# COMMAND ----------

Image(url="https://github.com/data-derp/exercise-data-security/blob/master/images/privacy_and_information_continuum.png?raw=true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulations!! 
# MAGIC
# MAGIC You've walked through potential privacy snags and helped increase the protection for the individuals sending you their air quality details! Now developers can use this dataset and we have ensured that there are some base protections. As you may have noticed, it wasn't always obvious what we should do -- but by thinking through each data type and determining what worked to balance the utility of the data and the privacy we want to offer, we were able to find some ways to protect individuals. 
# MAGIC
# MAGIC A good set of questions to ask for guidance is:
# MAGIC
# MAGIC - Where will this data be accessed and used? How safe is this environment?
# MAGIC - What person-related data do we actually need to use to deliver this service or product? (data minimization!)
# MAGIC - What other protections will be added to this data before it is seen or used? (i.e. encryption at rest, access control systems, or other protections when it reaches another processing point or sink!)
# MAGIC - What privacy and security expectations do we want to set for the individuals in this dataset?
# MAGIC - Where can we opportunistically add more protection while not hindering the work of data scientists, data analysts, software engineers and other colleagues?
# MAGIC
# MAGIC
# MAGIC As you continue on in your data engineering journey, you'll likely encounter many more situations where you'll need to make privacy and security decisions. If you'd like to learn more and even work as a privacy or security champion -- feel free to join in your organizations' programs to support topics like this!

# COMMAND ----------



# COMMAND ----------


