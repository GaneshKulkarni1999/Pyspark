#!/usr/bin/env python
# coding: utf-8

# Pyspark Dataframes
# 
# Filter Operation
# &,|,==
# ~
# 

# In[14]:


from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('filter').getOrCreate()
df=spark.read.csv(r"C:\Users\Ganesh Kulkarni\Documents\Dataset\Salary Data.csv",header=True,inferSchema=True)


# In[15]:


df.show()


# In[17]:


df.na.drop(how='any',thresh=3).show()


# In[18]:


df.na.drop(how='any',thresh=3).show()


# # filter Opearation
# 

# In[23]:


#Salary of the people less Than or equal to 20000

df.filter("Salary<=20000").show()


# In[58]:


df.filter("Salary>=30000").select(['Job Title','Education Level']).show()


# In[60]:


df.filter((df['Salary']<=30000) & (df['Salary']>35000)).show()


# In[61]:


df.filter(~(df['Salary']<=20000)).show()


# In[30]:


from pyspark.ml.feature import Imputer

imputer=Imputer(
    inputCols=['Age','Years of Experience','Salary'],
    outputCols=["{}_imputed".format(c) for c in ['Age','Years of Experience','Salary']]
).setStrategy("mean")

a=imputer.fit(df).transform(df)
a.show()


# In[32]:


df.describe().show()


# In[34]:


df.show()


# In[35]:


df.select(['Job Title','Salary']).show()


# In[39]:


##Rename column
df.withColumnRenamed('Salary','New-Salary').show()


# In[41]:


df.show()


# In[62]:


type(df)


# 

# In[ ]:




