#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Test").getOrCreate()


# In[3]:


spark


# In[9]:


df=spark.read.csv(r"C:\Users\Ganesh Kulkarni\Documents\ethx peoject folder\Dataset\salary Data.csv",header=True,inferSchema=True)


# In[10]:


df.show()


# In[11]:


df.printSchema()


# In[12]:


df.select("Education Level","Years of Experience").show()


# In[27]:


df.groupBy('Education Level').sum().show()


# In[13]:


df.groupBy('Education Level').sum().na.drop(how='all').show()


# In[30]:


df.groupBy('Age').sum().show()


# In[34]:


df.groupBy('Age').mean().na.drop(how="any").show()


# In[37]:


df.describe().na.drop(how="any").show()


# In[43]:


df.collect()


# In[49]:


df.take(10)


# In[16]:


df.toPandas()


# In[15]:


df.agg({"Salary":"sum"}).show()


# In[17]:


df.groupBy("Job Title").count().show()


# In[22]:


df.groupBy("Salary").count().na.drop(how='any').show()


# In[21]:


df.groupBy("Salary").max().na.drop(how="any").show()


# In[23]:


df.groupBy("Salary").sum().show()


# In[25]:


df.agg({"Salary":"sum"}).show()


# In[ ]:




